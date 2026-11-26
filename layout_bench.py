"""Benchmark WCM waveform table shapes in a day file (staging-layout plan §4, step 4).

Usage:
    cd <repo-root>
    physiomon/.venv/bin/python layout_bench.py                 # 40 and 700 beds, 60 min, 1- and 10-min batches
    physiomon/.venv/bin/python layout_bench.py --beds 40 --minutes 10 --batch-minutes 1

Shapes:
    single       one waveforms table (today's layout); bed via pv1 joined on msg_seq
    single_guid  the same, joined on the MSH-10 GUID instead of msg_seq
    long_bed     one table per bed: (msg_seq, ts, signal, samples)
    bed_signal   one table per bed + signal: (msg_seq, ts, samples)
    wide_bed     one table per bed, one BLOB column per signal: (msg_seq, ts, ecg_ii_1, ...)

Each run starts from an empty day file and writes `--minutes` of WCM for every bed,
one transaction per batch (pv1 + waveforms), then a final CHECKPOINT, which is
counted. Then it reopens the file READ_ONLY and times reads.

Synthetic waveforms, already packed: HL7 parsing and int16 packing cost the same for
every shape (benchmark.py measures them). The signal mix follows the real sample:
8–13 signals per bed out of 27, ECG 1,440 samples per 6 s message, pressures 720,
others 360, and about a third of the extra ECG leads entirely sentinel values.
"""

import argparse
import shutil
import statistics
import tempfile
import time
import uuid
from pathlib import Path

import duckdb
import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.compute as pc

ECG, PRESSURE, SLOW = 1440, 720, 360
BASE = [("ecg_i_1", ECG), ("ecg_ii_1", ECG), ("ecg_iii_1", ECG), ("ecg_v_1", ECG), ("pleth_1", SLOW), ("resp_1", SLOW)]
OPTIONAL = (
    [(f"ecg_{lead}_1", ECG) for lead in ("avr", "avl", "avf", "v1", "v2", "v3", "v4", "v5", "v6")]
    + [(s, PRESSURE) for s in ("art_1", "art_2", "art_4", "cvp_3", "pa_2", "icp_5", "lap_3", "rap_3", "uap_1")]
    + [("co2_1", SLOW), ("pleth_2", SLOW), ("resp_2", SLOW)]
)
MSG_S = 6              # one WCM message per bed every 6 s
SENTINEL = -32768      # an unused lead sends only this
DAY0 = "2026-01-01 00:00:00+00"
DAY0_US = 1_767_225_600_000_000
READ_SIGNAL = "ecg_ii_1"  # every bed has it
SHAPES = ["single", "single_guid", "long_bed", "bed_signal", "wide_bed"]


# ── Synthetic data ────────────────────────────────────────────────────────────

class Ward:
    """Fixed signal mix per bed; one 'slot' is one message from every bed."""

    def __init__(self, n_beds: int, seed: int = 0):
        rng = np.random.default_rng(seed)
        self.beds = [f"UNIT{b // 48}^B{b}" for b in range(n_beds)]
        bed, sig, length, unused = [], [], [], []
        for b in range(n_beds):
            extra = rng.choice(len(OPTIONAL), rng.integers(2, 8), replace=False)
            for name, n in BASE + [OPTIONAL[i] for i in extra]:
                bed.append(b)
                sig.append(name)
                length.append(n)
                unused.append(name.startswith("ecg") and name not in ("ecg_i_1", "ecg_ii_1") and rng.random() < 0.35)
        self.row_bed = np.array(bed)
        self.row_sig = np.array(sig)
        self.row_len = np.array(length)
        self.row_unused = np.array(unused)
        self.signals = sorted(set(sig))

    def samples_per_slot(self) -> int:
        return int(self.row_len.sum())


def make_pool(n: int, seed: int = 1) -> np.ndarray:
    """ECG-like int16: a 240-sample sine plus noise, so compression is neither trivial nor hopeless."""
    rng = np.random.default_rng(seed)
    sine = (1000 * np.sin(np.arange(240) * 2 * np.pi / 240)).astype("<i2")
    return np.tile(sine, n // 240 + 1)[:n] + rng.integers(-30, 31, n, dtype=np.int16)


def make_batch(ward: Ward, slot0: int, n_slots: int, pool: np.ndarray, guids: pl.Series,
               rng: np.random.Generator, null_unused: bool) -> tuple[pl.DataFrame, pl.DataFrame]:
    """(pv1, waveforms) frames for slots [slot0, slot0 + n_slots), in arrival order."""
    n_beds, per_slot = len(ward.beds), len(ward.row_bed)
    slot = np.repeat(np.arange(slot0, slot0 + n_slots), per_slot)
    bed = np.tile(ward.row_bed, n_slots)
    msg = slot * n_beds + bed
    lens = np.tile(ward.row_len, n_slots)
    unused = np.tile(ward.row_unused, n_slots)

    start = int(rng.integers(0, len(pool) - lens.sum()))
    flat = pool[start:start + lens.sum()].copy()
    flat[np.repeat(unused, lens)] = SENTINEL
    offsets = np.concatenate([[0], np.cumsum(lens)]).astype(np.int64) * 2
    blobs = pa.LargeBinaryArray.from_buffers(pa.large_binary(), len(lens), [None, pa.py_buffer(offsets), pa.py_buffer(flat)])
    if null_unused:
        blobs = pc.if_else(pa.array(~unused), blobs, pa.scalar(None, pa.large_binary()))

    ts_us = DAY0_US + slot * MSG_S * 1_000_000 + bed * (MSG_S * 1_000_000 // n_beds)
    beds = pl.Series(ward.beds)
    waves = pl.DataFrame({
        "msg_seq": (msg + 1).astype(np.uint64),
        "msg_ctrl_id": guids.gather(msg),
        "bed": beds.gather(bed),
        "ts": pl.Series(ts_us).cast(pl.Datetime("us", "UTC")),
        "signal": pl.Series(np.tile(ward.row_sig, n_slots)),
        "samples": pl.Series(blobs, dtype=pl.Binary),
    })
    first = np.concatenate([[True], msg[1:] != msg[:-1]])
    pv1 = waves.filter(pl.Series(first)).select("msg_seq", "msg_ctrl_id", patient_location="bed")
    return pv1, waves


# ── Shapes ────────────────────────────────────────────────────────────────────

def _insert(con: duckdb.DuckDBPyConnection, table: str, df: pl.DataFrame) -> None:
    con.register("_b", df.to_arrow())  # same path as service._append
    con.execute(f"INSERT INTO {table} BY NAME SELECT * FROM _b")
    con.unregister("_b")


def _range(lo_min: float, hi_min: float) -> str:
    return (f"ts >= TIMESTAMPTZ '{DAY0}' + INTERVAL {int(lo_min * 60)} SECOND "
            f"AND ts < TIMESTAMPTZ '{DAY0}' + INTERVAL {int(hi_min * 60)} SECOND")


class Single:
    """Writer and reader SQL for one shape. Writers keep their table map in memory;
    readers rebuild it from the map table stored in the day file."""

    def __init__(self, con: duckdb.DuckDBPyConnection, key: str = "msg_seq"):
        self.con, self.key = con, key

    def create(self) -> None:
        kind = "UBIGINT" if self.key == "msg_seq" else "VARCHAR"
        self.con.execute(f"CREATE TABLE d.waveforms ({self.key} {kind}, ts TIMESTAMPTZ, signal VARCHAR, samples BLOB)")

    def write(self, w: pl.DataFrame) -> None:
        _insert(self.con, "d.waveforms", w.select(self.key, "ts", "signal", "samples"))

    def open(self) -> None:
        self.con.execute(
            "CREATE VIEW waves_all AS SELECT p.patient_location AS bed, w.ts, w.signal, w.samples "
            f"FROM d.waveforms w JOIN d.pv1 p USING ({self.key})")

    def bed_signal_sql(self, bed: str, signal: str, where: str) -> str:
        return (f"SELECT w.ts, w.samples FROM d.waveforms w JOIN d.pv1 p USING ({self.key}) "
                f"WHERE p.patient_location = '{bed}' AND w.signal = '{signal}' AND w.{where}")


class LongBed(Single):
    cols = "msg_seq UBIGINT, ts TIMESTAMPTZ, signal VARCHAR, samples BLOB"
    by = ("bed",)

    def create(self) -> None:
        self.map: dict[tuple, str] = {}
        keys = ", ".join(f"{k} VARCHAR" for k in self.by)
        self.con.execute(f"CREATE TABLE d.tables ({keys}, tbl VARCHAR)")

    def _table(self, key: tuple) -> str:
        if key not in self.map:
            self.map[key] = t = f"w{len(self.map)}"
            self.con.execute(f"CREATE TABLE d.{t} ({self.cols})")
            self.con.execute(f"INSERT INTO d.tables VALUES ({', '.join('?' * (len(key) + 1))})", [*key, t])
        return self.map[key]

    def write(self, w: pl.DataFrame) -> None:
        for key, part in w.partition_by(self.by, as_dict=True).items():
            _insert(self.con, f"d.{self._table(key)}", part.select("msg_seq", "ts", "signal", "samples"))

    def _load(self) -> list[tuple]:
        rows = self.con.execute("SELECT * FROM d.tables").fetchall()
        self.map = {tuple(r[:-1]): r[-1] for r in rows}
        return rows

    def open(self) -> None:
        union = " UNION ALL ".join(f"SELECT '{bed}' AS bed, ts, signal, samples FROM d.{t}" for bed, t in self._load())
        self.con.execute(f"CREATE VIEW waves_all AS {union}")

    def bed_signal_sql(self, bed: str, signal: str, where: str) -> str:
        return f"SELECT ts, samples FROM d.{self.map[(bed,)]} WHERE signal = '{signal}' AND {where}"


class BedSignal(LongBed):
    cols = "msg_seq UBIGINT, ts TIMESTAMPTZ, samples BLOB"
    by = ("bed", "signal")

    def write(self, w: pl.DataFrame) -> None:
        for key, part in w.partition_by(self.by, as_dict=True).items():
            _insert(self.con, f"d.{self._table(key)}", part.select("msg_seq", "ts", "samples"))

    def open(self) -> None:
        union = " UNION ALL ".join(f"SELECT '{bed}' AS bed, ts, '{sig}' AS signal, samples FROM d.{t}"
                                   for bed, sig, t in self._load())
        self.con.execute(f"CREATE VIEW waves_all AS {union}")

    def bed_signal_sql(self, bed: str, signal: str, where: str) -> str:
        return f"SELECT ts, samples FROM d.{self.map[(bed, signal)]} WHERE {where}"


class WideBed(LongBed):
    cols = "msg_seq UBIGINT, ts TIMESTAMPTZ"

    def create(self) -> None:
        super().create()
        self.columns: dict[str, set[str]] = {}

    def write(self, w: pl.DataFrame) -> None:
        wide = w.pivot(on="signal", index=["bed", "msg_seq", "ts"], values="samples")
        for (bed,), part in wide.partition_by("bed", as_dict=True).items():
            part = part.select("msg_seq", "ts", *(c for c in part.columns[3:] if part[c].null_count() < part.height))
            t = self._table((bed,))
            have = self.columns.setdefault(t, set())
            for c in part.columns[2:]:
                if c not in have:  # a signal appears for the first time on this bed
                    self.con.execute(f'ALTER TABLE d.{t} ADD COLUMN "{c}" BLOB')
                    have.add(c)
            _insert(self.con, f"d.{t}", part)

    def open(self) -> None:
        union = " UNION ALL BY NAME ".join(f"SELECT '{bed}' AS bed, * FROM d.{t}" for bed, t in self._load())
        self.con.execute(f"CREATE VIEW waves_all AS {union}")

    def bed_signal_sql(self, bed: str, signal: str, where: str) -> str:
        return f'SELECT ts, "{signal}" AS samples FROM d.{self.map[(bed,)]} WHERE {where}'


def _shape(name: str, con: duckdb.DuckDBPyConnection) -> Single:
    return {
        "single": lambda: Single(con),
        "single_guid": lambda: Single(con, key="msg_ctrl_id"),
        "long_bed": lambda: LongBed(con),
        "bed_signal": lambda: BedSignal(con),
        "wide_bed": lambda: WideBed(con),
    }[name]()


# ── Runs ──────────────────────────────────────────────────────────────────────

def _timed(fn, repeats: int) -> tuple[float, float, int]:
    """(first, median of the rest, result rows); first includes planning a fresh query."""
    t0 = time.perf_counter()
    rows = fn().num_rows
    first = time.perf_counter() - t0
    rest = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        fn()
        rest.append(time.perf_counter() - t0)
    return first, statistics.median(rest), rows


def run(shape_name: str, ward: Ward, minutes: int, batch_minutes: int, pool: np.ndarray, guids: pl.Series,
        work: Path, threshold: str, null_unused: bool, repeats: int) -> dict:
    path = work / "20260101.duckdb"
    con = duckdb.connect(config={"checkpoint_threshold": threshold})
    con.execute("SET TimeZone = 'UTC'")
    con.execute(f"ATTACH '{path.as_posix()}' AS d")
    con.execute("CREATE TABLE d.pv1 (msg_seq UBIGINT, msg_ctrl_id VARCHAR, patient_location VARCHAR)")
    shape = _shape(shape_name, con)
    shape.create()

    rng = np.random.default_rng(2)
    slots_per_batch, n_slots = batch_minutes * 60 // MSG_S, minutes * 60 // MSG_S
    batch_s, waves_bytes = [], 0
    for slot0 in range(0, n_slots, slots_per_batch):
        pv1, waves = make_batch(ward, slot0, min(slots_per_batch, n_slots - slot0), pool, guids, rng, null_unused)
        waves_bytes += waves["samples"].bin.size().sum() or 0
        t0 = time.perf_counter()
        con.begin()
        _insert(con, "d.pv1", pv1)
        shape.write(waves)
        con.commit()
        batch_s.append(time.perf_counter() - t0)
    t0 = time.perf_counter()
    con.execute("CHECKPOINT d")
    checkpoint_s = time.perf_counter() - t0
    tables = con.execute("SELECT count(*) FROM duckdb_tables() WHERE database_name = 'd'").fetchone()[0]
    con.close()

    n_msgs = n_slots * len(ward.beds)
    ingest_s = sum(batch_s) + checkpoint_s
    file_mb = sum(f.stat().st_size for f in work.glob("*.duckdb*")) / 1e6

    # Reads: a fresh session, as a reader of a closed day file would open it
    t0 = time.perf_counter()
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.execute(f"ATTACH '{path.as_posix()}' AS d (READ_ONLY)")
    reader = _shape(shape_name, con)
    reader.open()
    open_s = time.perf_counter() - t0
    bed = ward.beds[len(ward.beds) // 2]
    hour = reader.bed_signal_sql(bed, READ_SIGNAL, _range(0, min(60, minutes)))
    mid = minutes / 2
    window = f"SELECT * FROM waves_all WHERE {_range(mid, mid + min(5, minutes / 2))}"
    hour_first, hour_med, hour_rows = _timed(lambda: con.sql(hour).to_arrow_table(), repeats)
    win_first, win_med, win_rows = _timed(lambda: con.sql(window).to_arrow_table(), repeats)
    con.close()

    return {
        "beds": len(ward.beds), "batch_min": batch_minutes, "shape": shape_name,
        "msg_s": n_msgs / ingest_s, "x_real": minutes * 60 / ingest_s,
        "batch_p50": statistics.median(batch_s), "batch_max": max(batch_s), "checkpoint_s": checkpoint_s,
        "tables": tables, "file_mb": file_mb, "mb_per_bed_hour": file_mb / len(ward.beds) / (minutes / 60),
        "raw_per_bed_hour": waves_bytes / 1e6 / len(ward.beds) / (minutes / 60), "open_ms": open_s * 1e3,
        "hour_first_ms": hour_first * 1e3, "hour_ms": hour_med * 1e3, "hour_rows": hour_rows,
        "win_first_ms": win_first * 1e3, "win_ms": win_med * 1e3, "win_rows": win_rows,
    }


COLUMNS = [  # (header, key, format)
    ("Beds", "beds", "{}"), ("Batch (min)", "batch_min", "{}"), ("Shape", "shape", "{}"),
    ("msg/s", "msg_s", "{:,.0f}"), ("× real time", "x_real", "{:,.0f}"),
    ("Batch p50 (s)", "batch_p50", "{:.2f}"), ("Batch max (s)", "batch_max", "{:.2f}"),
    ("Checkpoint (s)", "checkpoint_s", "{:.1f}"), ("Tables", "tables", "{:,}"),
    ("File (MB)", "file_mb", "{:,.0f}"), ("MB/bed-hour", "mb_per_bed_hour", "{:.1f}"), ("Samples MB/bed-hour", "raw_per_bed_hour", "{:.1f}"),
    ("Open (ms)", "open_ms", "{:,.0f}"),
    ("Bed-hour 1st (ms)", "hour_first_ms", "{:,.1f}"), ("Bed-hour (ms)", "hour_ms", "{:,.1f}"),
    ("Window 1st (ms)", "win_first_ms", "{:,.0f}"), ("Window (ms)", "win_ms", "{:,.0f}"),
]


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--beds", type=int, nargs="+", default=[40, 700])
    p.add_argument("--minutes", type=int, default=60, help="simulated minutes per run (reads use up to 60)")
    p.add_argument("--batch-minutes", type=int, nargs="+", default=[1, 10], help="simulated minutes per transaction")
    p.add_argument("--shapes", nargs="+", default=SHAPES, choices=SHAPES)
    p.add_argument("--checkpoint-threshold", default="1GB")
    p.add_argument("--null-unused", action="store_true", help="store all-sentinel rows as NULL instead of a BLOB")
    p.add_argument("--repeats", type=int, default=5)
    p.add_argument("--work-dir", type=Path, default=Path(tempfile.gettempdir()))
    args = p.parse_args()

    print(f"| {' | '.join(h for h, _, _ in COLUMNS)} |\n|{' --- |' * len(COLUMNS)}", flush=True)
    for n_beds in args.beds:
        ward = Ward(n_beds)
        pool = make_pool(ward.samples_per_slot() * max(args.batch_minutes) * 60 // MSG_S + (1 << 20))
        n_msgs = n_beds * args.minutes * 60 // MSG_S
        guids = pl.Series([str(uuid.uuid4()) for _ in range(n_msgs)])
        for batch_minutes in args.batch_minutes:
            for shape in args.shapes:
                work = Path(tempfile.mkdtemp(prefix="layout_bench_", dir=args.work_dir))
                try:
                    r = run(shape, ward, args.minutes, batch_minutes, pool, guids, work,
                            args.checkpoint_threshold, args.null_unused, args.repeats)
                finally:
                    shutil.rmtree(work, ignore_errors=True)
                print(f"| {' | '.join(f.format(r[k]) for _, k, f in COLUMNS)} |", flush=True)


if __name__ == "__main__":
    main()
