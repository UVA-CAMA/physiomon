"""Command-line entry point: `uv run bench [OPTIONS]`.

For each reader count, starts a fresh writer process plus N reader processes and
reports ingest speed (as a speedup over real time), query latency and freshness.
Then probes ATTACH visibility and runs the crash test: kill the writer mid-ingest,
reopen the day files, and compare row counts with what the writer logged as committed.
"""

import argparse
import json
import shutil
import statistics
import subprocess
import sys
import time
from pathlib import Path

import duckdb

from qbench.reader import QUERIES, probe_attach
from qbench.writer import DAYS


def _start_writer(work_dir: Path, args: argparse.Namespace, seconds: float, threshold: str) -> subprocess.Popen:
    cmd = [sys.executable, "-m", "qbench.writer", "--dir", str(work_dir), "--uri", args.uri,
           "--seconds", str(seconds), "--log", str(work_dir / "writer.jsonl"),
           "--beds", str(args.beds), "--msgs-per-bed", str(args.msgs_per_bed),
           "--checkpoint-threshold", threshold]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True)
    if proc.stdout.readline().strip() != "READY":
        proc.kill()
        sys.exit("writer failed to start")
    return proc


def _fresh_dir(path: Path) -> Path:
    shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True)
    return path


def _log(work_dir: Path) -> tuple[list[dict], float]:
    """Committed batches, and the final checkpoint time (0 if the writer was killed)."""
    lines = [json.loads(line) for line in (work_dir / "writer.jsonl").read_text().splitlines()]
    final = [x["final_checkpoint_s"] for x in lines if "final_checkpoint_s" in x]
    return [x for x in lines if "batch" in x], sum(final)


def _q(values: list[float], p: float) -> float:
    return statistics.quantiles(values, n=100)[p - 1] if len(values) > 1 else (values[0] if values else float("nan"))


def run_scenario(args: argparse.Namespace, threshold: str, n_readers: int) -> dict:
    work_dir = _fresh_dir(args.work_dir / "scenario")  # one at a time; WAL-heavy runs use a lot of disk
    writer = _start_writer(work_dir, args, args.seconds + 2, threshold)  # outlive the readers
    readers = [
        subprocess.Popen([sys.executable, "-m", "qbench.reader", "--uri", args.uri,
                          "--seconds", str(args.seconds), "--out", str(work_dir / f"reader{r}.json")])
        for r in range(n_readers)
    ]
    attach = probe_attach(args.uri) if n_readers == 0 else None
    for p in readers:
        p.wait()
    writer.wait()

    log, final_checkpoint_s = _log(work_dir)
    write_s = sum(b["seconds"] for b in log) + final_checkpoint_s
    reads = [json.loads((work_dir / f"reader{r}.json").read_text()) for r in range(n_readers)]
    lat = {q: [x for rd in reads for x in rd["latencies"][q]] for q in QUERIES}
    stale = [x for rd in reads for x in rd["staleness"]]
    shutil.rmtree(work_dir, ignore_errors=True)
    return {
        "threshold": threshold,
        "readers": n_readers,
        "batches": len(log),
        "speedup": sum(b["sim_seconds"] for b in log) / write_s,
        "write_mb_s": sum(b["wave_bytes"] for b in log) / write_s / 1e6,
        "batch_p95_s": _q([b["seconds"] for b in log], 95),
        "final_checkpoint_s": final_checkpoint_s,
        "wal_max_mb": max(b["wal_bytes"] for b in log) / 1e6,
        "lat_p50_ms": {q: 1000 * _q(v, 50) for q, v in lat.items()},
        "lat_p95_ms": {q: 1000 * _q(v, 95) for q, v in lat.items()},
        "stale_p95_s": _q(stale, 95) if stale else None,
        "errors": [e for rd in reads for e in rd["errors"]],
        "rollover": sorted({b["day"] for b in log}),
        "attach": attach,
    }


def crash_test(args: argparse.Namespace, trial: int) -> str:
    work_dir = _fresh_dir(args.work_dir / f"crash{trial}")
    writer = _start_writer(work_dir, args, 3600, args.checkpoint_thresholds[-1])  # biggest WAL to replay
    time.sleep(args.crash_after * (1 + trial / 2))  # vary the kill point; later trials cross the rollover
    writer.kill()  # SIGKILL / TerminateProcess: no cleanup, no checkpoint
    writer.wait()
    log, _ = _log(work_dir)
    logged = sum(b["numerics_rows"] for b in log)
    per_batch = log[0]["numerics_rows"] if log else 0
    found = 0
    for day in DAYS:
        f = work_dir / f"{day}.duckdb"
        if f.exists():
            with duckdb.connect(str(f)) as con:  # replays the WAL
                found += con.execute("SELECT count(*) FROM numerics").fetchall()[0][0]
    # A kill between COMMIT and the log write leaves one committed batch unlogged
    ok = found in (logged, logged + per_batch)
    return f"trial {trial}: logged {logged:,} rows, found {found:,} after reopen -> {'OK' if ok else 'MISMATCH'}"


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="Benchmark live Quack reads against a DuckDB ingest.")
    ap.add_argument("--readers", type=int, nargs="+", default=[0, 1, 4])
    ap.add_argument("--checkpoint-thresholds", nargs="+", default=["16MB", "256MB", "1GB"],
                    help="WAL size that triggers an automatic checkpoint; DuckDB's default is 16MB")
    ap.add_argument("--seconds", type=float, default=30, help="duration of each scenario")
    ap.add_argument("--beds", type=int, default=193)
    ap.add_argument("--msgs-per-bed", type=int, default=10, help="messages per bed per batch (6 s each)")
    ap.add_argument("--crash-trials", type=int, default=3)
    ap.add_argument("--crash-after", type=float, default=3.0, help="seconds before the first kill")
    ap.add_argument("--uri", default="quack:localhost:9494")
    ap.add_argument("--work-dir", type=Path, default=Path("./bench_data"))
    args = ap.parse_args(argv)

    results = [run_scenario(args, t, n) for t in args.checkpoint_thresholds for n in args.readers]
    base = results[0]["speedup"]
    print(f"\n{'ckpt at':>7} {'readers':>7} {'batches':>7} {'speedup':>8} {'vs base':>7} {'wave MB/s':>9} {'batch p95':>9} "
          f"{'final ckpt':>10} {'WAL max':>8} {'stale p95':>9}  query p50 / p95 ms")
    for r in results:
        lat = "  ".join(f"{q} {r['lat_p50_ms'][q]:.0f}/{r['lat_p95_ms'][q]:.0f}" for q in QUERIES) if r["readers"] else "-"
        stale = f"{r['stale_p95_s']:.1f}s" if r["stale_p95_s"] is not None else "-"
        print(f"{r['threshold']:>7} {r['readers']:>7} {r['batches']:>7} {r['speedup']:>7.0f}x {r['speedup'] / base:>6.0%} "
              f"{r['write_mb_s']:>9.0f} {r['batch_p95_s']:>8.2f}s {r['final_checkpoint_s']:>9.2f}s "
              f"{r['wal_max_mb']:>6.0f}MB {stale:>9}  {lat}")
    for r in results:
        if r["errors"]:
            print(f"{r['threshold']} readers={r['readers']}: {len(r['errors'])} query errors, first: {r['errors'][0]}")
        if len(r["rollover"]) < 2:
            print(f"{r['threshold']} readers={r['readers']}: run too short to reach the day rollover")
    print(f"\nATTACH from a client: {results[0]['attach']}")
    print("\nCrash test (kill the writer, reopen the day files):")
    for t in range(args.crash_trials):
        print("  " + crash_test(args, t))
