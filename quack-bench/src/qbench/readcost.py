"""Read overhead: the same queries over the same day files, direct vs over Quack.

    uv run bench-read [--batches 20] [--repeats 10]

Builds two day files once (no ingest running), then times each query three ways:
- direct:      this process ATTACHes the day files READ_ONLY (how any reader opens a
               day file nobody is writing);
- quack_query: a server process holds the files and runs the query; rows come back
               over Quack;
- attach:      the client ATTACHes the server and queries its views.

Results are fetched as Arrow so Python object conversion doesn't dominate. MB/s is
the size of the fetched Arrow result over the median time.
"""

import argparse
import shutil
import statistics
import subprocess
import sys
import time
from pathlib import Path

import duckdb

from qbench.datagen import make_batch
from qbench.writer import DAYS, TOKEN, _attach_day, _replace_views

QUERIES = {
    "count":         "SELECT count(*) FROM numerics_all",
    "bed_numerics":  "SELECT obs_code, avg(value) FROM numerics_all WHERE bed = 'UNIT0^BED3' GROUP BY 1",
    "all_numerics":  "SELECT bed, obs_code, avg(value), max(value) FROM numerics_all GROUP BY ALL",
    "numerics_rows": "SELECT * FROM numerics_all",
    "bed_waves":     "SELECT ts, signal, samples FROM waves_all WHERE bed = 'UNIT0^BED3'",
    "unit_waves":    "SELECT ts, bed, signal, samples FROM waves_all WHERE bed LIKE 'UNIT0^%'",
}


def build(work_dir: Path, batches: int, beds: int, msgs_per_bed: int) -> None:
    batch = make_batch(beds, msgs_per_bed, 10, 9, 1440)
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.register("num", batch.numerics)
    con.register("wav", batch.waves)
    for d, day in enumerate(DAYS):
        _attach_day(con, work_dir, day)
        for i in range(d * batches, (d + 1) * batches):
            seq0, ts0_us = i * batch.n_msgs, int(i * batch.sim_seconds * 1e6)
            con.execute(f"INSERT INTO {day}.numerics SELECT ? + msg, bed, "
                        "to_timestamp((? + offset_us) / 1e6), obs_code, value, now() FROM num", [seq0, ts0_us])
            con.execute(f"INSERT INTO {day}.waves SELECT ? + msg, bed, "
                        "to_timestamp((? + offset_us) / 1e6), signal, samples FROM wav", [seq0, ts0_us])
        con.execute(f"DETACH {day}")  # checkpoints
    con.close()


def _open_days(con: duckdb.DuckDBPyConnection, work_dir: Path) -> None:
    for day in DAYS:
        con.execute(f"ATTACH '{(work_dir / f'{day}.duckdb').as_posix()}' AS {day} (READ_ONLY)")
    _replace_views(con, list(DAYS))


def serve(work_dir: Path, uri: str) -> None:
    con = duckdb.connect()
    con.execute("LOAD quack")
    _open_days(con, work_dir)
    con.execute(f"CALL quack_serve('{uri}', token = '{TOKEN}')")
    print("READY", flush=True)
    sys.stdin.read()  # serve until the parent closes stdin


def _time(fn, repeats: int) -> tuple[float, int]:
    fn()  # warm-up: file cache, connection, plan
    times = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        table = fn()
        times.append(time.perf_counter() - t0)
    return statistics.median(times), table.nbytes


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="Query cost over Quack vs reading the day files directly.")
    ap.add_argument("--batches", type=int, default=20, help="batches per day file (1 simulated minute each)")
    ap.add_argument("--beds", type=int, default=193)
    ap.add_argument("--msgs-per-bed", type=int, default=10)
    ap.add_argument("--repeats", type=int, default=10)
    ap.add_argument("--uri", default="quack:localhost:9495")
    ap.add_argument("--work-dir", type=Path, default=Path("./bench_data"))
    args = ap.parse_args(argv)

    work_dir = args.work_dir / "readcost"
    shutil.rmtree(work_dir, ignore_errors=True)
    work_dir.mkdir(parents=True)
    t0 = time.perf_counter()
    build(work_dir, args.batches, args.beds, args.msgs_per_bed)
    size = sum(f.stat().st_size for f in work_dir.glob("*.duckdb")) / 1e6
    print(f"built {len(DAYS)} day files, {size:,.0f} MB, in {time.perf_counter() - t0:.0f} s")

    server = subprocess.Popen([sys.executable, "-m", "qbench.readcost", "--serve", str(work_dir), "--uri", args.uri],
                              stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    if server.stdout.readline().strip() != "READY":
        server.kill()
        sys.exit("server failed to start")
    try:
        direct = duckdb.connect()
        _open_days(direct, work_dir)
        client = duckdb.connect()
        client.execute("LOAD quack")
        client.execute(f"ATTACH '{args.uri}' AS r (TOKEN '{TOKEN}')")
        modes = {
            "direct": lambda sql: direct.sql(sql).to_arrow_table(),
            "quack_query": lambda sql: client.sql(
                "SELECT * FROM quack_query(?, ?, token := ?)", params=[args.uri, sql, TOKEN]).to_arrow_table(),
            "attach": lambda sql: client.sql(sql.replace("FROM ", "FROM r.", 1)).to_arrow_table(),
        }
        print(f"\nmedian of {args.repeats}; ms (MB/s of fetched result); x = time vs direct")
        print(f"{'query':<14} {'result':>9}  " + "  ".join(f"{m:>22}" for m in modes))
        for name, sql in QUERIES.items():
            res = {m: _time(lambda: fn(sql), args.repeats) for m, fn in modes.items()}
            base = res["direct"][0]
            nbytes = res["direct"][1]
            cells = [f"{s * 1000:>8.1f} ms ({nbytes / s / 1e6:>5.0f}) {s / base:>4.1f}x" for s, _ in res.values()]
            print(f"{name:<14} {nbytes / 1e6:>7.1f}MB  " + "  ".join(f"{c:>22}" for c in cells))
    finally:
        server.stdin.close()
        server.wait()
        shutil.rmtree(work_dir, ignore_errors=True)


if __name__ == "__main__":
    if "--serve" in sys.argv:
        ap = argparse.ArgumentParser()
        ap.add_argument("--serve", type=Path, required=True)
        ap.add_argument("--uri", required=True)
        a = ap.parse_args()
        serve(a.serve, a.uri)
    else:
        main()
