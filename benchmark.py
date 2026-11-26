"""Benchmark service.ingest_batch throughput by message type and batch size (on-disk day files).

Usage:
    cd <repo-root>
    python benchmark.py              # 500 messages per type
    python benchmark.py --count 2000
"""

import argparse
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "physiomon"))
sys.path.insert(0, str(ROOT / "physiosim"))

import duckdb

import service
from generator import generate_dec, generate_acm, generate_wcm, make_patients


def _messages(feed: str, count: int) -> list[bytes]:
    p = make_patients(1)[0]
    gen = {"DEC": generate_dec, "ACM": generate_acm, "WCM": generate_wcm}[feed]
    return [gen(p).encode() for _ in range(count)]


def _rows(staging_dir: str) -> int:
    con = duckdb.connect()
    files = list(Path(staging_dir).rglob("*.duckdb"))
    for i, f in enumerate(files):
        con.execute(f"ATTACH '{f.as_posix()}' AS f{i} (READ_ONLY)")
    total = sum(
        con.execute(f"SELECT count(*) FROM f{i}.{t}").fetchone()[0]
        for i in range(len(files)) for t in ("msh", "obr", "numerics", "waveforms", "signals", "alarms")
    )
    con.close()
    return total


def _bench(label: str, messages: list[bytes], staging_dir: str, batch_size: int) -> None:
    store = service.Staging(staging_dir)
    n = len(messages)

    t0 = time.perf_counter()
    for i in range(0, n, batch_size):
        service.ingest_batch(store, messages[i:i + batch_size], list(range(i + 1, min(i + batch_size, n) + 1)))
    elapsed = time.perf_counter() - t0
    store.close()
    total_rows = _rows(staging_dir)

    print(
        f"  {label:<34}  {n / elapsed:>7.0f} msg/s  "
        f"{total_rows / elapsed:>9.0f} rows/s  "
        f"({elapsed:.2f}s, {total_rows} rows)"
    )


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--count", type=int, default=500)
    p.add_argument("--batch-sizes", type=str, default="10,50,200")
    args = p.parse_args()

    batch_sizes = [int(x) for x in args.batch_sizes.split(",")]
    print(f"Benchmarking — {args.count} messages per type\n")

    for feed in ("DEC", "ACM", "WCM"):
        msgs = _messages(feed, args.count)
        with tempfile.TemporaryDirectory() as d:
            _bench(f"{feed} one-at-a-time", msgs, str(Path(d) / "one"), 1)
            for bs in batch_sizes:
                _bench(f"{feed} batch={bs:<4}", msgs, str(Path(d) / f"batch{bs}"), bs)
        print()


if __name__ == "__main__":
    main()
