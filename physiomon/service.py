"""Long-running NATS subscriber: ingests HL7 messages into one DuckDB file per UTC day.

Public surface:
    Staging(dir)                    — day-file store: staging/YYYYMMDD.duckdb, attached on demand
    Staging.serve(listen, token)    — serve the session over Quack: live SQL on attached days
    ingest_batch(store, msgs, seqs) — parse a batch, write each day's rows in one transaction
    ingest(store, raw, seq)         — single-message wrapper (tests)
    open_store(dir)                 — Staging for a long-running consumer, served over Quack if configured
    run(...)                        — async subscriber loop, blocks until SIGINT/SIGTERM
"""

from __future__ import annotations

import asyncio
import datetime
import logging
import signal
from pathlib import Path

import duckdb
import nats
import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
from nats.js.api import ConsumerConfig, DeliverPolicy, StreamConfig

import config
from parser import ParsedMessage, parse_vectorized

log = logging.getLogger(__name__)

# ── Table schemas ─────────────────────────────────────────────────────────────

_UTC = pl.Datetime("us", "UTC")

_MSH: dict = {
    "msg_seq":            pl.UInt64, "msg_ctrl_id":       pl.String,
    "received_at":        _UTC,      "msg_ts":            _UTC,
    "feed":               pl.String,
    "sending_app":        pl.String,
    "sending_facility":   pl.String, "receiving_app":     pl.String,
    "receiving_facility": pl.String, "msg_timestamp":     pl.String,
    "msg_type":           pl.String, "processing_id":     pl.String,
    "version_id":         pl.String, "accept_ack_type":   pl.String,
    "app_ack_type":       pl.String, "msg_profile":       pl.String,
}
_PID: dict = {
    "msg_seq":        pl.UInt64,
    "patient_id_list": pl.String, "patient_name":   pl.String,
    "dob":            pl.String, "admin_sex":       pl.String,
    "account_number": pl.String,
}
_PV1: dict = {
    "msg_seq":         pl.UInt64,
    "patient_class":   pl.String, "patient_location": pl.String,
    "visit_number":    pl.String, "admit_datetime":   pl.String,
    "visit_indicator": pl.String,
}
_OBR: dict = {
    "msg_seq":          pl.UInt64,
    "obr_set_id":       pl.String, "placer_order_num": pl.String,
    "filler_order_num": pl.String, "service_id":       pl.String,
    "obs_start_time":   pl.String, "obs_end_time":     pl.String,
    "parent_number":    pl.String,
}
_OBX_BASE: dict = {
    "msg_seq":        pl.UInt64, "obr_set_id":     pl.String,
    "obx_set_id":     pl.String, "value_type":     pl.String,
    "obs_identifier": pl.String, "obs_code":       pl.String,
    "obs_name":       pl.String, "sub_id":         pl.String,
    "channel":        pl.Int32,
}
_OBX_TAIL: dict = {
    "units":         pl.String, "ref_range":     pl.String,
    "abn_flag":      pl.String, "result_status": pl.String,
    "obs_datetime":  pl.String, "equipment_id":  pl.String,
    "obs_site":      pl.String,
}

_SCHEMAS: dict[str, dict] = {
    "msh":       _MSH,
    "pid":       _PID,
    "pv1":       _PV1,
    "obr":       _OBR,
    "numerics":  {**_OBX_BASE, "feed": pl.String, "value": pl.String, **_OBX_TAIL},
    # samples: packed little-endian int16. value: the raw text, kept only when it
    # doesn't decode losslessly (empty or non-int16 elements); NULL otherwise.
    "waveforms": {**_OBX_BASE, "samples": pl.Binary, "value": pl.String, **_OBX_TAIL},
    # Per-channel waveform attributes (sample rate, data range, sentinels, resolution):
    # the NM/NR rows of an OBR group that carries a waveform.
    "signals":   {**_OBX_BASE, "value": pl.String, **_OBX_TAIL},
    "alarms":    {**_OBX_BASE, "value": pl.String, **_OBX_TAIL},
}


# ── Day files ─────────────────────────────────────────────────────────────────

class Staging:
    """One DuckDB file per UTC day, ATTACHed to one in-memory session as needed.

    Today and yesterday (UTC wall clock) stay attached; older days are reopened for
    late data and detached after the batch. A day that was created here once but whose
    file is gone (moved off the drive) goes to late/YYYYMMDD.duckdb instead, so a
    partial file never silently reappears at the old path.
    """

    def __init__(self, directory: str | Path):
        self.dir = Path(directory)
        if self.dir.is_file():  # e.g. the old single-file PHYSIOMON_STAGING_DB path
            raise SystemExit(f"staging dir {self.dir} is a file; set PHYSIOMON_STAGING_DIR to a folder")
        self.dir.mkdir(parents=True, exist_ok=True)
        db_config = {"checkpoint_threshold": config.CHECKPOINT_THRESHOLD}
        if config.DUCKDB_EXTENSION_DIR:
            db_config["extension_directory"] = config.DUCKDB_EXTENSION_DIR
        self.con = duckdb.connect(config=db_config)
        self.con.execute("SET TimeZone = 'UTC'")
        self._manifest = self.dir / "days.txt"  # every day this store has created
        self._created = set(self._manifest.read_text().split()) if self._manifest.exists() else set()
        self.attached: dict[str, str] = {}  # day (YYYYMMDD) -> catalog alias

    def path(self, day: str) -> Path:
        main = self.dir / f"{day}.duckdb"
        return main if main.exists() or day not in self._created else self.dir / "late" / f"{day}.duckdb"

    def attach(self, day: str) -> str:
        if day in self.attached:
            return self.attached[day]
        path = self.path(day)
        if day in self._created and path.parent.name == "late" and not path.exists():
            log.warning("day %s was moved off the drive; writing late data to %s", day, path)
        path.parent.mkdir(parents=True, exist_ok=True)
        alias = f"{'late' if path.parent.name == 'late' else 'd'}{day}"
        self.con.execute(f"ATTACH '{path.as_posix()}' AS {alias}")
        for table, schema in _SCHEMAS.items():
            self.con.register("_tmpl", pl.DataFrame(schema=schema).to_arrow())
            self.con.execute(f"CREATE TABLE IF NOT EXISTS {alias}.{table} AS SELECT * FROM _tmpl LIMIT 0")
            self.con.unregister("_tmpl")
        if day not in self._created:
            self._created.add(day)
            with self._manifest.open("a") as f:
                f.write(day + "\n")
            log.info("created day file %s", path)
        self.attached[day] = alias
        self._replace_views()
        return alias

    def _replace_views(self) -> None:
        """`<table>_all` views over every attached day, in the default catalog where
        Quack clients that ATTACH can see them."""
        aliases = sorted(self.attached.values())
        for table in _SCHEMAS:
            if aliases:
                union = " UNION ALL BY NAME ".join(f"SELECT * FROM {a}.{table}" for a in aliases)
                self.con.execute(f"CREATE OR REPLACE VIEW memory.main.{table}_all AS {union}")
            else:
                self.con.execute(f"DROP VIEW IF EXISTS memory.main.{table}_all")

    def detach_old(self, now: datetime.datetime | None = None) -> None:
        """Detach all but today and yesterday (UTC)."""
        today = (now or datetime.datetime.now(datetime.UTC)).date()
        keep = {f"{d:%Y%m%d}" for d in (today, today - datetime.timedelta(days=1))}
        old = {d: self.attached.pop(d) for d in list(self.attached) if d not in keep}
        if old:
            self._replace_views()  # before DETACH, so a view never names a detached day
        for alias in old.values():
            self.con.execute(f"DETACH {alias}")

    def serve(self, listen: str, token: str) -> None:
        """Serve this session over DuckDB Quack, so other processes can query the
        attached days (`numerics_all`, or `d20260101.numerics`) while ingest runs.
        Readers see committed batches only."""
        try:
            self.con.execute("LOAD quack")
        except duckdb.Error:
            self.con.execute("INSTALL quack")  # needs internet; offline: DUCKDB_EXTENSION_DIR
            self.con.execute("LOAD quack")
        self.con.execute(f"CALL quack_serve({_sql_str(listen)}, token = {_sql_str(token)})")
        log.info("serving Quack on %s", listen)

    def close(self) -> None:
        self.con.close()


def _sql_str(v: str) -> str:
    return "'" + v.replace("'", "''") + "'"


def _append(con: duckdb.DuckDBPyConnection, table: str, df: pl.DataFrame) -> None:
    if df.is_empty():
        return
    con.register("_append", df.to_arrow())
    con.execute(f"INSERT INTO {table} BY NAME SELECT * FROM _append")
    con.unregister("_append")


# ── Batch transforms ──────────────────────────────────────────────────────────

def _pack_int16(waveforms: pl.DataFrame) -> pl.DataFrame:
    """'^'-delimited sample text in `value` -> packed little-endian int16 BLOBs in `samples`.

    No row loop. A row is lossless when every element parses as int16; then `value`
    is set to NULL. Otherwise unparseable elements are packed as 0 and the text is kept.
    """
    # One frame-level select: Polars parallelises it (Series methods run sequentially)
    parsed = waveforms.select(
        ints=pl.col("value").str.split("^").list.eval(pl.element().cast(pl.Int16, strict=False)),
    ).with_columns(
        lossless=pl.col("ints").list.eval(pl.element().is_not_null()).list.all(),
    )
    lists = parsed["ints"].to_arrow()
    if isinstance(lists, pa.ChunkedArray):
        lists = lists.combine_chunks()
    data = np.asarray(lists.flatten().fill_null(0)).astype("<i2", copy=False)
    offsets = np.asarray(lists.offsets).astype(np.int64)
    offsets = (offsets - offsets[0]) * 2
    blobs = pa.LargeBinaryArray.from_buffers(
        pa.large_binary(), len(lists), [None, pa.py_buffer(offsets), pa.py_buffer(data)]
    )
    blobs = pc.if_else(lists.is_valid(), blobs, pa.scalar(None, pa.large_binary()))
    return waveforms.with_columns(
        pl.Series("samples", blobs, dtype=pl.Binary),
        value=pl.when(parsed["lossless"]).then(None).otherwise(pl.col("value")),
    )


def _split(parsed: ParsedMessage, received_at: list[datetime.datetime] | None) -> dict[str, pl.DataFrame]:
    """Per-table frames for one batch; msh gets a `day` column (YYYYMMDD) for routing."""
    obx = parsed.obx
    is_wave = pl.col("value_type") == "NA"
    wave_groups = obx.filter(is_wave).select("msg_seq", "obr_set_id").unique()
    received = pl.Series(
        "received_at",
        received_at if received_at is not None else [datetime.datetime.now(datetime.UTC)] * parsed.msh.height,
        dtype=_UTC,
    )
    msh = (
        parsed.msh.with_columns(received)
        .with_columns(
            feed=pl.when(pl.col("msg_type").str.contains("R40")).then(pl.lit("ACM"))
            .when(pl.col("msg_seq").is_in(wave_groups["msg_seq"].implode())).then(pl.lit("WCM"))
            .otherwise(pl.lit("DEC")),
            # Day routing clock: the message's own timestamp, else NATS arrival time
            day=pl.coalesce("msg_ts", "received_at").dt.strftime("%Y%m%d"),
        )
    )

    numeric = obx.filter(pl.col("value_type").is_in(["NM", "NR"]))
    waveforms = obx.filter(is_wave)
    if not waveforms.is_empty():
        waveforms = _pack_int16(waveforms)
    return {
        "msh":       msh,
        "pid":       parsed.pid,
        "pv1":       parsed.pv1,
        "obr":       parsed.obr,
        "numerics":  numeric.join(wave_groups, on=["msg_seq", "obr_set_id"], how="anti")
                     .join(msh.select("msg_seq", "feed"), on="msg_seq"),
        "waveforms": waveforms,
        "signals":   numeric.join(wave_groups, on=["msg_seq", "obr_set_id"], how="semi"),
        "alarms":    obx.filter(pl.col("value_type") == "ST"),
    }


def ingest_batch(
    store: Staging,
    messages: list[bytes],
    seqs: list[int],
    received_at: list[datetime.datetime] | None = None,
) -> None:
    """Parse a batch and write it to its day files, one transaction per day.

    DuckDB writes to one attached file per transaction, so a batch spanning midnight
    commits twice. Messages whose msg_seq a day file already holds are skipped, which
    makes redelivery after a partial failure (or a crash before the ACK) idempotent.
    """
    frames = _split(parse_vectorized(messages, seqs), received_at)
    for (day,), msh in frames["msh"].partition_by("day", as_dict=True).items():
        alias = store.attach(day)
        con = store.con
        con.begin()
        try:
            lo, hi = msh["msg_seq"].min(), msh["msg_seq"].max()
            done = [r[0] for r in con.execute(
                f"SELECT msg_seq FROM {alias}.msh WHERE msg_seq BETWEEN ? AND ?", [lo, hi]).fetchall()]
            new = msh.filter(~pl.col("msg_seq").is_in(done))["msg_seq"]
            whole = len(new) == frames["msh"].height  # usual case: one day, nothing stored yet
            for table, df in frames.items():
                rows = df if whole else df.filter(pl.col("msg_seq").is_in(new.implode()))
                if not rows.is_empty():
                    _append(con, f"{alias}.{table}", rows.select(list(_SCHEMAS[table])))
            con.commit()
        except Exception:
            con.rollback()
            raise
        if done:
            log.info("skipped %d already-stored messages in %s", len(done), day)
    store.detach_old()


def ingest(store: Staging, raw: bytes, seq: int) -> None:
    """Single-message wrapper around ingest_batch — used by tests."""
    ingest_batch(store, [raw], [seq])


def open_store(staging_dir: str) -> Staging:
    """Staging for a long-running consumer, served over Quack when PHYSIOMON_QUACK_LISTEN is set."""
    store = Staging(staging_dir)
    if config.QUACK_LISTEN:
        if not config.QUACK_TOKEN:
            raise SystemExit("PHYSIOMON_QUACK_LISTEN is set but PHYSIOMON_QUACK_TOKEN is not")
        store.attach(f"{datetime.datetime.now(datetime.UTC):%Y%m%d}")  # readers have views from the start
        store.serve(config.QUACK_LISTEN, config.QUACK_TOKEN)
    return store


# ── NATS ──────────────────────────────────────────────────────────────────────

async def _ensure_stream(js, stream: str, subject: str) -> None:
    try:
        await js.stream_info(stream)
    except nats.js.errors.NotFoundError:
        await js.add_stream(StreamConfig(
            name=stream, subjects=[subject],
            compression="s2", max_bytes=10 * 1024 ** 3,
        ))
        log.info("created stream %r", stream)


async def run(
    nats_url:   str        = config.NATS_URL,
    stream:     str        = config.STREAM,
    subject:    str        = config.SUBJECT,
    durable:    str | None = config.CONSUMER_DURABLE,
    staging_dir: str       = config.STAGING_DIR,
) -> None:
    """Subscribe to NATS JetStream and ingest to DuckDB until SIGINT/SIGTERM."""
    store = open_store(staging_dir)
    stop = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # Windows: event loop has no signal support
            signal.signal(sig, lambda *_: loop.call_soon_threadsafe(stop.set))

    nc = await nats.connect(nats_url)
    js = nc.jetstream()
    await _ensure_stream(js, stream, subject)
    sub = await js.pull_subscribe(
        subject,
        stream=stream,
        durable=durable,
        config=ConsumerConfig(deliver_policy=DeliverPolicy.ALL),
    )
    log.info("service started — subject=%s durable=%s dir=%s batch=%d",
             subject, durable, staging_dir, config.NATS_BATCH_SIZE)

    try:
        while not stop.is_set():
            try:
                msgs = await sub.fetch(config.NATS_BATCH_SIZE, timeout=1.0)
            except (TimeoutError, nats.errors.TimeoutError):
                continue
            try:
                meta = [m.metadata for m in msgs]
                ingest_batch(store, [m.data for m in msgs],
                             [x.sequence.stream for x in meta], [x.timestamp for x in meta])
                for m in msgs:
                    await m.ack()
            except Exception:
                log.exception("ingest_batch failed — not ACKing %d messages, will redeliver on restart",
                              len(msgs))
    finally:
        store.close()
        try:
            await sub.unsubscribe()
        except Exception:
            pass
        await nc.drain()
        log.info("service stopped")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    asyncio.run(run())
