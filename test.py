"""Tests for the physiomon pipeline.

Unit tests run unconditionally.
Integration tests require NATS and mllp2nats; they skip gracefully if absent.

Run from repo root via physiomon's venv:
    cd physiomon && uv run --extra test python -m pytest ../test.py -v
"""

import os
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "physiomon"))
sys.path.insert(0, str(ROOT / "physiosim"))

import duckdb
import pytest

import sender
import service
from config import MLLP_HOST, MLLP_PORT, NATS_URL, STREAM

PHYSIO    = ROOT / "physiomon"
_EXAMPLES = ROOT / "physiosim" / "examples"

_ALL_TABLES = {"msh", "pid", "pv1", "obr", "numerics", "waveforms", "alarms"}
_OBX_TABLES = {"numerics", "waveforms", "alarms"}


_CASE_FILES = str(_EXAMPLES / "case*.hl7")


# ── fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def con(tmp_path):
    c = service.open_staging_db(tmp_path / "staging.duckdb")
    yield c
    c.close()


@pytest.fixture
def con_with_data(con):
    for raw in sender.load_files(_CASE_FILES):
        service.ingest(con, raw)
    return con


def _listening(host, port) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


@pytest.fixture(scope="module")
def services_ready():
    import asyncio
    import nats as _nats
    async def _check():
        try:
            nc = await _nats.connect(NATS_URL)
            await nc.close()
            return True
        except Exception:
            return False
    if not asyncio.run(_check()):
        pytest.skip(f"NATS not reachable at {NATS_URL}")
    if not _listening(MLLP_HOST, MLLP_PORT):
        pytest.skip(f"mllp2nats not listening on {MLLP_HOST}:{MLLP_PORT}")
    return True


def _start_service(staging_db: Path, durable: str) -> subprocess.Popen:
    env = {
        **os.environ,
        "PHYSIOMON_STAGING_DB":       str(staging_db),
        "PHYSIOMON_CONSUMER_DURABLE": durable,
    }
    proc = subprocess.Popen(
        [sys.executable, str(PHYSIO / "service.py")],
        cwd=str(PHYSIO), env=env,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    time.sleep(2)
    return proc


def _delete_consumer(durable: str) -> None:
    subprocess.run(
        ["nats", "consumer", "rm", STREAM, durable, "--force"],
        capture_output=True,
    )


# ── unit tests ────────────────────────────────────────────────────────────────

def test_tables_created(con):
    tables = {
        r[0] for r in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
        ).fetchall()
    }
    assert tables == _ALL_TABLES


def test_tables_idempotent(tmp_path):
    path = tmp_path / "staging.duckdb"
    c1 = service.open_staging_db(path)
    c1.close()
    c2 = service.open_staging_db(path)
    tables = {
        r[0] for r in c2.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
        ).fetchall()
    }
    c2.close()
    assert tables == _ALL_TABLES


def test_ingest_populates_all_tables(con_with_data):
    for table in _OBX_TABLES:
        assert con_with_data.execute(f"SELECT count(*) FROM {table}").fetchone()[0] > 0
    for table in ("msh", "pv1", "obr"):
        assert con_with_data.execute(f"SELECT count(*) FROM {table}").fetchone()[0] > 0


def test_context_tables_have_correct_columns(con_with_data):
    assert con_with_data.execute("SELECT * FROM msh LIMIT 1").fetchone() is not None
    msh_cols = {r[0] for r in con_with_data.execute("DESCRIBE msh").fetchall()}
    assert {"msg_ctrl_id", "sending_app", "msg_timestamp", "msg_type", "msg_profile"} <= msh_cols
    pv1_cols = {r[0] for r in con_with_data.execute("DESCRIBE pv1").fetchall()}
    assert {"msg_ctrl_id", "patient_class", "patient_location"} <= pv1_cols
    obr_cols = {r[0] for r in con_with_data.execute("DESCRIBE obr").fetchall()}
    assert {"msg_ctrl_id", "obr_set_id", "placer_order_num", "service_id"} <= obr_cols


def test_obx_tables_have_no_context_columns(con):
    for table in _OBX_TABLES:
        cols = {r[0] for r in con.execute(f"DESCRIBE {table}").fetchall()}
        assert "bed_location"   not in cols
        assert "msg_timestamp"  not in cols
        assert "obs_start_time" not in cols


def test_waveform_values_are_lists(con_with_data):
    sample = con_with_data.execute("SELECT values FROM waveforms LIMIT 1").fetchone()
    assert sample is not None and isinstance(sample[0], list) and len(sample[0]) > 0


def test_waveform_values_are_int16(con_with_data):
    sample = con_with_data.execute("SELECT values FROM waveforms LIMIT 1").fetchone()[0]
    assert all(-32768 <= v <= 32767 for v in sample)


def test_numerics_have_channel(con_with_data):
    assert con_with_data.execute(
        "SELECT count(*) FROM numerics WHERE channel IS NOT NULL"
    ).fetchone()[0] > 0


def test_obx_join_to_context(con_with_data):
    """OBX rows can be joined back to their MSH and OBR context via msg_ctrl_id."""
    count = con_with_data.execute("""
        SELECT count(*)
        FROM numerics n
        JOIN msh      m ON n.msg_ctrl_id = m.msg_ctrl_id
        JOIN obr      o ON n.msg_ctrl_id = o.msg_ctrl_id
                       AND n.obr_set_id  = o.obr_set_id
    """).fetchone()[0]
    assert count > 0


def test_no_obx_rows_for_msh_only_message(con):
    service.ingest(con, b"MSH|^~\\&|DEC||||| 20260101120000||ORU^R01|uuid-1|P|2.6\n")
    for table in _OBX_TABLES:
        assert con.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 0
    assert con.execute("SELECT count(*) FROM msh").fetchone()[0] == 1


# ── integration tests (require NATS + mllp2nats) ─────────────────────────────

def test_end_to_end(services_ready, tmp_path):
    db_path = tmp_path / "staging.duckdb"
    durable = f"test-{uuid.uuid4().hex[:8]}"
    proc = _start_service(db_path, durable)
    try:
        sender.mllp_send(sender.load_files(_CASE_FILES), MLLP_HOST, MLLP_PORT)
        time.sleep(3)
    finally:
        proc.terminate()
        proc.wait(timeout=10)
        _delete_consumer(durable)

    con = duckdb.connect(str(db_path), read_only=True)
    for table in ("msh", "pv1", "obr", "numerics", "waveforms", "alarms"):
        assert con.execute(f"SELECT count(*) FROM {table}").fetchone()[0] > 0, \
            f"{table} empty after end-to-end run"
    wf_sample = con.execute("SELECT values FROM waveforms LIMIT 1").fetchone()
    assert wf_sample is not None and isinstance(wf_sample[0], list)
    con.close()


def test_restart_durability(services_ready, tmp_path):
    db_path = tmp_path / "staging.duckdb"
    durable = f"test-{uuid.uuid4().hex[:8]}"

    proc = _start_service(db_path, durable)
    try:
        sender.mllp_send(sender.load_files(_CASE_FILES), MLLP_HOST, MLLP_PORT)
        time.sleep(3)
    finally:
        proc.terminate()
        proc.wait(timeout=10)

    con = duckdb.connect(str(db_path), read_only=True)
    counts_first = {
        t: con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
        for t in ("numerics", "waveforms", "alarms")
    }
    con.close()

    proc2 = _start_service(db_path, durable)
    time.sleep(2)
    proc2.terminate()
    proc2.wait(timeout=10)
    _delete_consumer(durable)

    con2 = duckdb.connect(str(db_path), read_only=True)
    counts_second = {
        t: con2.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
        for t in ("numerics", "waveforms", "alarms")
    }
    con2.close()

    assert counts_second == counts_first, \
        f"rows changed after restart: {counts_first} → {counts_second}"
