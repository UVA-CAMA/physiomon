"""Tests for the physiomon pipeline.

Unit tests run unconditionally.
Integration tests require NATS and mllp2nats; they skip gracefully if absent.

Run from repo root via physiomon's venv:
    cd physiomon && uv run --extra test python -m pytest ../test.py -v
"""

import datetime
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
import numpy as np
import pytest

import sender
import service
from config import MLLP_HOST, MLLP_PORT, NATS_URL, STREAM
from generator import generate_acm, generate_dec, generate_wcm, make_patients

PHYSIO    = ROOT / "physiomon"
_EXAMPLES = ROOT / "physiosim" / "examples"

_ALL_TABLES = {"msh", "pid", "pv1", "obr", "numerics", "waveforms", "signals", "alarms"}
_OBX_TABLES = {"numerics", "waveforms", "signals", "alarms"}


_CASE_FILES = str(_EXAMPLES / "case*.hl7")
_UTC = datetime.UTC


def _read_all(staging_dir: Path) -> duckdb.DuckDBPyConnection:
    """Read-only session over every day file, with one view per table across days."""
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    files = sorted(staging_dir.rglob("*.duckdb"))
    for i, f in enumerate(files):
        con.execute(f"ATTACH '{f.as_posix()}' AS f{i} (READ_ONLY)")
    for t in _ALL_TABLES:
        union = " UNION ALL BY NAME ".join(f"SELECT * FROM f{i}.{t}" for i in range(len(files)))
        con.execute(f"CREATE VIEW {t} AS {union}")
    return con


def _hl7_utc(days_ago: int = 0) -> str:
    """An MSH-7 value at 00:00:01 UTC, `days_ago` days before today."""
    day = datetime.datetime.now(_UTC).date() - datetime.timedelta(days=days_ago)
    return f"{day:%Y%m%d}000001.0000+0000"


def _quack_count(reader: duckdb.DuckDBPyConnection, uri: str, token: str, table: str) -> int:
    return reader.execute("SELECT * FROM quack_query(?, ?, token := ?)",
                          [uri, f"SELECT count(*) FROM {table}", token]).fetchone()[0]


def _day_files(staging_dir: Path) -> list[str]:
    return sorted(str(f.relative_to(staging_dir)) for f in staging_dir.rglob("*.duckdb"))


def _wcm_at(ts: str) -> bytes:
    """A simulated WCM message with MSH-7 replaced by ts."""
    fields = generate_wcm(make_patients(1)[0]).split("|", 7)
    fields[6] = ts
    return "|".join(fields).encode()


# ── fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def store(tmp_path):
    s = service.Staging(tmp_path)
    yield s
    s.close()


@pytest.fixture
def con_with_data(tmp_path):
    # The example files are waveform messages only; add simulated vitals and alarms
    p = make_patients(1)[0]
    raws = sender.load_files(_CASE_FILES) + [generate_dec(p).encode(), generate_acm(p).encode()]
    s = service.Staging(tmp_path)
    service.ingest_batch(s, raws, list(range(1, len(raws) + 1)))
    s.close()
    c = _read_all(tmp_path)
    yield c
    c.close()


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


def _start_service(staging_dir: Path, durable: str, **extra_env: str) -> subprocess.Popen:
    env = {
        **os.environ,
        "PHYSIOMON_STAGING_DIR":      str(staging_dir),
        "PHYSIOMON_CONSUMER_DURABLE": durable,
        **extra_env,
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

def _tables(con, alias):
    return {
        r[0] for r in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_catalog = ?", [alias]
        ).fetchall()
    }


def test_tables_created(store):
    assert _tables(store.con, store.attach("20260101")) == _ALL_TABLES


def test_tables_idempotent(tmp_path):
    s1 = service.Staging(tmp_path)
    s1.attach("20260101")
    s1.close()
    s2 = service.Staging(tmp_path)
    tables = _tables(s2.con, s2.attach("20260101"))
    s2.close()
    assert tables == _ALL_TABLES


def test_ingest_populates_all_tables(con_with_data):
    for table in _OBX_TABLES:
        assert con_with_data.execute(f"SELECT count(*) FROM {table}").fetchone()[0] > 0, table
    for table in ("msh", "pv1", "obr"):
        assert con_with_data.execute(f"SELECT count(*) FROM {table}").fetchone()[0] > 0, table


def test_context_tables_have_correct_columns(con_with_data):
    assert con_with_data.execute("SELECT msg_seq FROM msh LIMIT 1").fetchone() is not None
    msh_cols = {r[0] for r in con_with_data.execute("DESCRIBE msh").fetchall()}
    assert {"msg_seq", "msg_ctrl_id", "received_at", "msg_ts", "feed", "sending_app",
            "msg_timestamp", "msg_type", "msg_profile"} <= msh_cols
    pv1_cols = {r[0] for r in con_with_data.execute("DESCRIBE pv1").fetchall()}
    assert {"msg_seq", "patient_class", "patient_location"} <= pv1_cols
    obr_cols = {r[0] for r in con_with_data.execute("DESCRIBE obr").fetchall()}
    assert {"msg_seq", "obr_set_id", "placer_order_num", "service_id"} <= obr_cols


def test_obx_tables_have_no_context_columns(con_with_data):
    for table in _OBX_TABLES:
        cols = {r[0] for r in con_with_data.execute(f"DESCRIBE {table}").fetchall()}
        assert "msg_ctrl_id"    not in cols
        assert "bed_location"   not in cols
        assert "msg_timestamp"  not in cols
        assert "obs_start_time" not in cols


def test_msg_ts_parsed_to_utc(con_with_data):
    bad = con_with_data.execute(
        "SELECT count(*) FROM msh WHERE msg_ts IS NULL AND msg_timestamp LIKE '%-____'"
    ).fetchone()[0]
    assert bad == 0
    raw, ts = con_with_data.execute(
        "SELECT msg_timestamp, msg_ts::VARCHAR FROM msh WHERE msg_timestamp LIKE '%-0400' LIMIT 1"
    ).fetchone()
    hour = int(raw[8:10])
    assert int(ts[11:13]) == (hour + 4) % 24


def test_feeds(con_with_data):
    feeds = {r[0] for r in con_with_data.execute("SELECT DISTINCT feed FROM msh").fetchall()}
    assert feeds <= {"DEC", "WCM", "ACM"} and len(feeds) > 1
    mismatched = con_with_data.execute(
        "SELECT count(*) FROM numerics n JOIN msh m USING (msg_seq) WHERE n.feed <> m.feed"
    ).fetchone()[0]
    assert mismatched == 0


def test_waveform_attributes_go_to_signals(con_with_data):
    in_numerics = con_with_data.execute(
        "SELECT count(*) FROM numerics WHERE obs_name LIKE 'MDC_ATTR_SAMPLE_RATE' OR obs_name LIKE 'MDC_EVT_DATA_%'"
    ).fetchone()[0]
    in_signals = con_with_data.execute(
        "SELECT count(*) FROM signals WHERE obs_name = 'MDC_ATTR_SAMPLE_RATE'"
    ).fetchone()[0]
    assert in_numerics == 0 and in_signals > 0
    # The vitals in a waveform message's "monitoring of patient" group stay in numerics
    wcm_vitals = con_with_data.execute(
        "SELECT count(*) FROM numerics WHERE feed = 'WCM' AND obs_name = 'MDC_ECG_HEART_RATE'"
    ).fetchone()[0]
    assert wcm_vitals > 0


def test_waveform_blob_round_trip(tmp_path):
    raw = generate_wcm(make_patients(1)[0]).encode()
    texts = service.parse_vectorized([raw]).obx.filter(
        service.pl.col("value_type") == "NA")["value"].to_list()
    s = service.Staging(tmp_path)
    service.ingest(s, raw, 1)
    s.close()
    con = _read_all(tmp_path)
    rows = con.execute("SELECT samples, value FROM waveforms ORDER BY obr_set_id::INT").fetchall()
    assert len(rows) == len(texts) > 0
    for (blob, value), text in zip(rows, texts):
        assert value is None
        assert np.frombuffer(blob, "<i2").tolist() == [int(x) for x in text.split("^")]


def test_lossy_waveform_keeps_text(tmp_path):
    raw = generate_wcm(make_patients(1)[0])
    i = raw.index("|NA|")
    j = raw.index("|", raw.index("|", i + 4) + 1) + 1  # start of OBX-5
    raw = (raw[:j] + "1^^40000^x^" + raw[j:]).encode()
    s = service.Staging(tmp_path)
    service.ingest(s, raw, 1)
    s.close()
    con = _read_all(tmp_path)
    blob, value = con.execute("SELECT samples, value FROM waveforms WHERE value IS NOT NULL").fetchone()
    assert value.startswith("1^^40000^x^")
    assert np.frombuffer(blob, "<i2")[:4].tolist() == [1, 0, 0, 0]


def test_numerics_have_channel(con_with_data):
    assert con_with_data.execute(
        "SELECT count(*) FROM numerics WHERE channel IS NOT NULL"
    ).fetchone()[0] > 0


def test_obx_join_to_context(con_with_data):
    """OBX rows can be joined back to their MSH and OBR context via msg_seq."""
    count = con_with_data.execute("""
        SELECT count(*)
        FROM numerics n
        JOIN msh      m ON n.msg_seq    = m.msg_seq
        JOIN obr      o ON n.msg_seq    = o.msg_seq
                       AND n.obr_set_id = o.obr_set_id
    """).fetchone()[0]
    assert count > 0


def test_no_obx_rows_for_msh_only_message(tmp_path):
    s = service.Staging(tmp_path)
    service.ingest(s, b"MSH|^~\\&|DEC||||| 20260101120000||ORU^R01|uuid-1|P|2.6\n", 1)
    s.close()
    con = _read_all(tmp_path)
    for table in _OBX_TABLES:
        assert con.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 0
    assert con.execute("SELECT count(*) FROM msh").fetchone()[0] == 1


def test_midnight_batch_routes_by_message_time(tmp_path):
    msgs = [_wcm_at("20260110235955.0000+0000"), _wcm_at("20260111000001.0000+0000"),
            _wcm_at("20260110190005.0000-0500"), _wcm_at("not a timestamp")]
    arrival = datetime.datetime(2026, 1, 12, 0, 0, 30, tzinfo=_UTC)  # used only by the last
    s = service.Staging(tmp_path)
    service.ingest_batch(s, msgs, [1, 2, 3, 4], [arrival] * 4)
    s.close()
    assert _day_files(tmp_path) == ["20260110.duckdb", "20260111.duckdb", "20260112.duckdb"]
    con = _read_all(tmp_path)
    by_day = con.execute(
        "SELECT strftime(coalesce(msg_ts, received_at), '%Y%m%d'), list(msg_seq ORDER BY msg_seq) "
        "FROM msh GROUP BY 1 ORDER BY 1"
    ).fetchall()
    assert by_day == [("20260110", [1]), ("20260111", [2, 3]), ("20260112", [4])]
    assert con.execute("SELECT count(DISTINCT msg_seq) FROM waveforms").fetchone()[0] == 4


def test_redelivery_is_idempotent(tmp_path):
    msgs = [_wcm_at("20260110235955.0000+0000"), _wcm_at("20260111000001.0000+0000")]
    s = service.Staging(tmp_path)
    service.ingest_batch(s, msgs, [1, 2])
    service.ingest_batch(s, msgs + [_wcm_at("20260111000007.0000+0000")], [1, 2, 3])
    s.close()
    con = _read_all(tmp_path)
    assert con.execute("SELECT count(*) FROM msh").fetchone()[0] == 3
    per_msg = con.execute("SELECT count(*) FROM waveforms GROUP BY msg_seq").fetchall()
    assert len({r[0] for r in per_msg}) == 1  # no message has doubled rows


def test_old_day_is_reopened_and_moved_day_goes_to_late(tmp_path):
    s = service.Staging(tmp_path)
    service.ingest_batch(s, [_wcm_at("20260110120000.0000+0000")], [1])
    assert s.attached == {}  # not today or yesterday: detached after the batch
    service.ingest_batch(s, [_wcm_at("20260110120006.0000+0000")], [2])  # reopened
    (tmp_path / "20260110.duckdb").rename(tmp_path / "moved.bak")  # tiered off the drive
    service.ingest_batch(s, [_wcm_at("20260110120012.0000+0000")], [3])
    s.close()
    con = duckdb.connect(str(tmp_path / "moved.bak"), read_only=True)
    assert con.execute("SELECT list(msg_seq ORDER BY 1) FROM msh").fetchone()[0] == [1, 2]
    con.close()
    con = duckdb.connect(str(tmp_path / "late" / "20260110.duckdb"), read_only=True)
    assert con.execute("SELECT list(msg_seq) FROM msh").fetchone()[0] == [3]
    con.close()


def test_views_span_attached_days(store):
    msgs = [_wcm_at(_hl7_utc(0)), _wcm_at(_hl7_utc(1)), _wcm_at(_hl7_utc(5))]
    service.ingest_batch(store, msgs, [1, 2, 3])
    # The 5-day-old file was reopened for its message, then detached; the views follow
    assert sorted(store.attached) == sorted(_hl7_utc(d)[:8] for d in (0, 1))
    assert store.con.execute("SELECT list(msg_seq ORDER BY msg_seq) FROM msh_all").fetchone()[0] == [1, 2]
    assert store.con.execute("SELECT count(*) FROM waveforms_all").fetchone()[0] > 0


def test_quack_serves_live_reads(store):
    uri, token = "quack:localhost:9496", "test-token"
    try:
        store.serve(uri, token)
    except duckdb.Error as e:
        pytest.skip(f"Quack unavailable: {e}")
    reader = duckdb.connect()
    reader.execute("LOAD quack")
    service.ingest_batch(store, [_wcm_at(_hl7_utc())], [1])
    assert _quack_count(reader, uri, token, "msh_all") == 1
    service.ingest_batch(store, [_wcm_at(_hl7_utc())], [2])
    assert _quack_count(reader, uri, token, "msh_all") == 2
    reader.execute(f"ATTACH '{uri}' AS live (TOKEN '{token}')")
    assert reader.execute("SELECT count(*) FROM live.waveforms_all").fetchone()[0] > 0
    reader.close()


# ── integration tests (require NATS + mllp2nats) ─────────────────────────────

def test_end_to_end(services_ready, tmp_path):
    durable = f"test-{uuid.uuid4().hex[:8]}"
    proc = _start_service(tmp_path, durable)
    p = make_patients(1)[0]
    try:
        sender.mllp_send(sender.load_files(_CASE_FILES) + [generate_dec(p).encode(), generate_acm(p).encode()],
                         MLLP_HOST, MLLP_PORT)
        time.sleep(3)
    finally:
        proc.terminate()
        proc.wait(timeout=10)
        _delete_consumer(durable)

    con = _read_all(tmp_path)
    for table in ("msh", "pv1", "obr", "numerics", "waveforms", "signals", "alarms"):
        assert con.execute(f"SELECT count(*) FROM {table}").fetchone()[0] > 0, \
            f"{table} empty after end-to-end run"
    wf_sample = con.execute("SELECT samples FROM waveforms LIMIT 1").fetchone()
    assert wf_sample is not None and len(wf_sample[0]) > 0
    con.close()


def test_live_reads_while_service_runs(services_ready, tmp_path):
    durable = f"test-{uuid.uuid4().hex[:8]}"
    uri, token = "quack:localhost:9498", "test-token"
    proc = _start_service(tmp_path, durable, PHYSIOMON_QUACK_LISTEN=uri, PHYSIOMON_QUACK_TOKEN=token)
    try:
        # Current timestamps: the views cover today and yesterday (the example files are older)
        sender.mllp_send([generate_wcm(make_patients(1)[0]).encode()], MLLP_HOST, MLLP_PORT)
        time.sleep(3)
        reader = duckdb.connect()
        reader.execute("LOAD quack")
        assert _quack_count(reader, uri, token, "waveforms_all") > 0
        assert proc.poll() is None, "service exited"
        reader.close()
    finally:
        proc.terminate()
        proc.wait(timeout=10)
        _delete_consumer(durable)


def test_restart_durability(services_ready, tmp_path):
    durable = f"test-{uuid.uuid4().hex[:8]}"

    proc = _start_service(tmp_path, durable)
    try:
        sender.mllp_send(sender.load_files(_CASE_FILES), MLLP_HOST, MLLP_PORT)
        time.sleep(3)
    finally:
        proc.terminate()
        proc.wait(timeout=10)

    con = _read_all(tmp_path)
    counts_first = {
        t: con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
        for t in ("numerics", "waveforms", "alarms")
    }
    con.close()

    proc2 = _start_service(tmp_path, durable)
    time.sleep(2)
    proc2.terminate()
    proc2.wait(timeout=10)
    _delete_consumer(durable)

    con2 = _read_all(tmp_path)
    counts_second = {
        t: con2.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
        for t in ("numerics", "waveforms", "alarms")
    }
    con2.close()

    assert counts_second == counts_first, \
        f"rows changed after restart: {counts_first} → {counts_second}"
