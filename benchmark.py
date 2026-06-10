"""Benchmark service.ingest throughput by message type and backend.

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

import service
from generator import generate_dec, generate_acm, generate_wcm, make_patients


def _messages(feed: str, count: int) -> list[bytes]:
    p = make_patients(1)[0]
    gen = {"DEC": generate_dec, "ACM": generate_acm, "WCM": generate_wcm}[feed]
    return [gen(p).encode() for _ in range(count)]


def _bench_one(label: str, messages: list[bytes], db_path: str) -> None:
    con = service.open_staging_db(db_path)
    n = len(messages)

    t0 = time.perf_counter()
    for raw in messages:
        service.ingest(con, raw)
    elapsed = time.perf_counter() - t0

    total_rows = sum(
        con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
        for t in ("msh", "obr", "numerics", "waveforms", "alarms")
    )
    con.close()

    print(
        f"  {label:<34}  {n / elapsed:>7.0f} msg/s  "
        f"{total_rows / elapsed:>9.0f} rows/s  "
        f"({elapsed:.2f}s, {total_rows} rows)"
    )


def _bench_batch(label: str, messages: list[bytes], db_path: str, batch_size: int) -> None:
    con = service.open_staging_db(db_path)
    n = len(messages)

    t0 = time.perf_counter()
    for i in range(0, n, batch_size):
        service.ingest_batch(con, messages[i:i + batch_size])
    elapsed = time.perf_counter() - t0

    total_rows = sum(
        con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
        for t in ("msh", "obr", "numerics", "waveforms", "alarms")
    )
    con.close()

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
            _bench_one(f"{feed} one-at-a-time  in-memory", msgs, ":memory:")
            _bench_one(f"{feed} one-at-a-time  on-disk  ", msgs, str(Path(d) / "one.duckdb"))
            for bs in batch_sizes:
                _bench_batch(f"{feed} batch={bs:<4} in-memory", msgs, ":memory:", bs)
                _bench_batch(f"{feed} batch={bs:<4} on-disk  ", msgs, str(Path(d) / f"batch{bs}.duckdb"), bs)
        print()


if __name__ == "__main__":
    main()
