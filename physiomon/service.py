"""Long-running NATS subscriber: ingests HL7 messages into DuckDB staging.

Public surface:
    open_staging_db(path)  — connect/create DuckDB file with tables ensured
    ingest(con, raw)       — parse one message, append rows to all staging tables (thin wrapper for tests)
    ingest_batch(con, msgs) — parse a batch, append rows in one transaction
    run(...)               — async subscriber loop, blocks until SIGINT/SIGTERM
"""

from __future__ import annotations

import asyncio
import logging
import signal
from pathlib import Path

import duckdb
import nats
import polars as pl
from nats.js.api import ConsumerConfig, DeliverPolicy, StreamConfig

import config
from parser import ParsedMessage, parse_vectorized

log = logging.getLogger(__name__)

# ── Table schemas ─────────────────────────────────────────────────────────────

_MSH: dict = {
    "msg_ctrl_id":        pl.String, "sending_app":       pl.String,
    "sending_facility":   pl.String, "receiving_app":     pl.String,
    "receiving_facility": pl.String, "msg_timestamp":     pl.String,
    "msg_type":           pl.String, "processing_id":     pl.String,
    "version_id":         pl.String, "accept_ack_type":   pl.String,
    "app_ack_type":       pl.String, "msg_profile":       pl.String,
}
_PID: dict = {
    "msg_ctrl_id":    pl.String,
    "patient_id_list": pl.String, "patient_name":   pl.String,
    "dob":            pl.String, "admin_sex":       pl.String,
    "account_number": pl.String,
}
_PV1: dict = {
    "msg_ctrl_id":     pl.String,
    "patient_class":   pl.String, "patient_location": pl.String,
    "visit_number":    pl.String, "admit_datetime":   pl.String,
    "visit_indicator": pl.String,
}
_OBR: dict = {
    "msg_ctrl_id":      pl.String,
    "obr_set_id":       pl.String, "placer_order_num": pl.String,
    "filler_order_num": pl.String, "service_id":       pl.String,
    "obs_start_time":   pl.String, "obs_end_time":     pl.String,
    "parent_number":    pl.String,
}
_OBX_BASE: dict = {
    "msg_ctrl_id":    pl.String, "obr_set_id":     pl.String,
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
    "numerics":  {**_OBX_BASE, "value":  pl.String,         **_OBX_TAIL},
    "waveforms": {**_OBX_BASE, "values": pl.List(pl.Int16), **_OBX_TAIL},
    "alarms":    {**_OBX_BASE, "value":  pl.String,         **_OBX_TAIL},
}


# ── Database ──────────────────────────────────────────────────────────────────

def open_staging_db(path: str | Path) -> duckdb.DuckDBPyConnection:
    """Connect to (or create) the staging DuckDB file, creating tables if absent."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    existing = {
        row[0]
        for row in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
    }
    for table, schema in _SCHEMAS.items():
        if table not in existing:
            tmpl = pl.DataFrame(schema=schema).to_arrow()
            con.register("_tmpl", tmpl)
            con.execute(f"CREATE TABLE {table} AS SELECT * FROM _tmpl LIMIT 0")
            con.unregister("_tmpl")
            log.info("created table %s", table)
    return con


def _append(con: duckdb.DuckDBPyConnection, table: str, df: pl.DataFrame) -> None:
    if df.is_empty():
        return
    arrow = df.to_arrow()
    con.register("_append", arrow)
    con.execute(f"INSERT INTO {table} BY NAME SELECT * FROM _append")
    con.unregister("_append")


def ingest_batch(con: duckdb.DuckDBPyConnection, messages: list[bytes]) -> None:
    """Parse a batch of HL7 messages and append all rows in one transaction."""
    parsed: ParsedMessage = parse_vectorized(messages)

    numerics = waveforms = alarms = pl.DataFrame()
    if not parsed.obx.is_empty():
        numerics  = parsed.obx.filter(pl.col("value_type").is_in(["NM", "NR"]))
        alarms    = parsed.obx.filter(pl.col("value_type") == "ST")
        waveforms = parsed.obx.filter(pl.col("value_type") == "NA")
        if not waveforms.is_empty():
            waveforms = (
                waveforms
                .with_columns(
                    pl.col("value")
                    .str.split("^")
                    .list.eval(pl.element().cast(pl.Int16, strict=False))
                    .alias("values")
                )
                .drop("value")
            )

    con.begin()
    try:
        _append(con, "msh",       parsed.msh)
        _append(con, "pid",       parsed.pid)
        _append(con, "pv1",       parsed.pv1)
        _append(con, "obr",       parsed.obr)
        _append(con, "numerics",  numerics)
        _append(con, "waveforms", waveforms)
        _append(con, "alarms",    alarms)
        con.commit()
    except Exception:
        con.rollback()
        raise


def ingest(con: duckdb.DuckDBPyConnection, raw: bytes) -> None:
    """Single-message wrapper around ingest_batch — used by tests."""
    ingest_batch(con, [raw])


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
    staging_db: str        = config.STAGING_DB,
) -> None:
    """Subscribe to NATS JetStream and ingest to DuckDB until SIGINT/SIGTERM."""
    con = open_staging_db(staging_db)
    stop = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    nc = await nats.connect(nats_url)
    js = nc.jetstream()
    await _ensure_stream(js, stream, subject)
    sub = await js.pull_subscribe(
        subject,
        stream=stream,
        durable=durable,
        config=ConsumerConfig(deliver_policy=DeliverPolicy.NEW),
    )
    log.info("service started — subject=%s durable=%s db=%s batch=%d",
             subject, durable, staging_db, config.NATS_BATCH_SIZE)

    try:
        while not stop.is_set():
            try:
                msgs = await sub.fetch(config.NATS_BATCH_SIZE, timeout=1.0)
            except (TimeoutError, nats.errors.TimeoutError):
                continue
            try:
                ingest_batch(con, [m.data for m in msgs])
                for m in msgs:
                    await m.ack()
            except Exception:
                log.exception("ingest_batch failed — not ACKing %d messages, will redeliver on restart",
                              len(msgs))
    finally:
        con.close()
        try:
            await sub.unsubscribe()
        except Exception:
            pass
        await nc.drain()
        log.info("service stopped")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    asyncio.run(run())
