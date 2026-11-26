"""Writer process: ingest batches into day files as fast as possible while serving Quack.

Mirrors the planned service: one in-memory DuckDB session ATTACHes one file per day,
serves itself over Quack, and commits one transaction per batch. Halfway through the
run it rolls over to the next day's file and keeps the previous day attached.

    python -m qbench.writer --dir bench_data --seconds 30 --log writer.jsonl

Prints READY once serving. Appends one JSON line per committed batch to --log, so a
killed writer leaves a record of what it committed.
"""

import argparse
import json
import time
from pathlib import Path

import duckdb

from qbench.datagen import make_batch

DAYS = ("d20000101", "d20000102")
TOKEN = "qbench-token"


def _attach_day(con: duckdb.DuckDBPyConnection, work_dir: Path, day: str) -> None:
    con.execute(f"ATTACH '{(work_dir / f'{day}.duckdb').as_posix()}' AS {day}")
    con.execute(f"CREATE TABLE IF NOT EXISTS {day}.numerics "
                "(msg_seq UBIGINT, bed VARCHAR, ts TIMESTAMPTZ, obs_code INTEGER, value DOUBLE, written_at TIMESTAMPTZ)")
    con.execute(f"CREATE TABLE IF NOT EXISTS {day}.waves "
                "(msg_seq UBIGINT, bed VARCHAR, ts TIMESTAMPTZ, signal INTEGER, samples BLOB)")


def _replace_views(con: duckdb.DuckDBPyConnection, days: list[str]) -> None:
    """Readers query memory.numerics_all / waves_all, which span every attached day."""
    for table in ("numerics", "waves"):
        union = " UNION ALL ".join(f"SELECT * FROM {d}.{table}" for d in days)
        con.execute(f"CREATE OR REPLACE VIEW memory.main.{table}_all AS {union}")


def run(work_dir: Path, uri: str, seconds: float, log: Path, beds: int, msgs_per_bed: int,
        numerics: int, signals: int, samples: int, checkpoint_threshold: str) -> None:
    batch = make_batch(beds, msgs_per_bed, numerics, signals, samples)
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.execute("LOAD quack")
    # WAL size that triggers an automatic checkpoint (DuckDB default 16MB)
    con.execute(f"SET checkpoint_threshold = '{checkpoint_threshold}'")
    _attach_day(con, work_dir, DAYS[0])
    attached = [DAYS[0]]
    _replace_views(con, attached)
    con.execute(f"CALL quack_serve('{uri}', token = '{TOKEN}')")
    print("READY", flush=True)

    w = con.cursor()
    w.register("num", batch.numerics)
    w.register("wav", batch.waves)
    start = time.perf_counter()
    i = 0
    with log.open("a") as out:
        while (elapsed := time.perf_counter() - start) < seconds:
            if elapsed > seconds / 2 and len(attached) == 1:  # day rollover
                _attach_day(con, work_dir, DAYS[1])
                attached.append(DAYS[1])
                _replace_views(con, attached)
            day = attached[-1]
            seq0, ts0_us = i * batch.n_msgs, int(i * batch.sim_seconds * 1e6)
            t0 = time.perf_counter()
            w.execute("BEGIN")
            w.execute(f"INSERT INTO {day}.numerics SELECT ? + msg, bed, "
                      "to_timestamp((? + offset_us) / 1e6), obs_code, value, now() FROM num", [seq0, ts0_us])
            w.execute(f"INSERT INTO {day}.waves SELECT ? + msg, bed, "
                      "to_timestamp((? + offset_us) / 1e6), signal, samples FROM wav", [seq0, ts0_us])
            w.execute("COMMIT")
            dt = time.perf_counter() - t0
            wal = sum(f.stat().st_size for f in work_dir.glob("*.duckdb.wal"))
            out.write(json.dumps({"batch": i, "day": day, "seconds": dt, "sim_seconds": batch.sim_seconds,
                                  "numerics_rows": batch.numerics.num_rows, "wave_rows": batch.waves.num_rows,
                                  "wave_bytes": batch.wave_bytes, "wal_bytes": wal}) + "\n")
            out.flush()
            i += 1
        # Automatic checkpoints can be postponed while readers hold transactions, so
        # count the deferred work: checkpoint every day file once readers are gone.
        con.execute(f"CALL quack_stop('{uri}')")
        t0 = time.perf_counter()
        for day in attached:
            con.execute(f"CHECKPOINT {day}")
        out.write(json.dumps({"final_checkpoint_s": time.perf_counter() - t0}) + "\n")
    con.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", type=Path, required=True)
    ap.add_argument("--uri", default="quack:localhost:9494")
    ap.add_argument("--seconds", type=float, default=30)
    ap.add_argument("--log", type=Path, required=True)
    ap.add_argument("--beds", type=int, default=193)
    ap.add_argument("--msgs-per-bed", type=int, default=10)
    ap.add_argument("--numerics", type=int, default=10)
    ap.add_argument("--signals", type=int, default=9)
    ap.add_argument("--samples", type=int, default=1440)
    ap.add_argument("--checkpoint-threshold", default="16MB")
    a = ap.parse_args()
    run(a.dir, a.uri, a.seconds, a.log, a.beds, a.msgs_per_bed, a.numerics, a.signals, a.samples,
        a.checkpoint_threshold)
