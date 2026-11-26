"""Export staging tables to de-identified zstd parquet: <out>/<table>.parquet.

Every column is listed in COLUMNS with an action; an unlisted column or table
aborts the export, so new parser fields can't leak by default.

    keep     copied as-is (bed location, codes, values, waveform samples, message keys)
    drop     removed (name, date of birth)
    ts       parsed as an HL7 timestamp, converted to UTC, shifted back by whole days
    tstz     already a UTC timestamp (msg_ts, received_at), shifted back by whole days
    <kind>   replaced by a random pseudonym, e.g. MRN -> P000042

Dates are shifted per bed (pv1.patient_location): each bed's first message lands
on 2000-01-01, keeping UTC time of day. Messages with no bed share one shift.

The re-identification keys (bed shifts and pseudonyms) go to --keys, which must
stay on the server. Later exports reuse them, so a bed keeps its shift and a
patient keeps its pseudonym across exports. Reads every day file (or --db files) as
one dataset. Stop service.py first.

    python export_deid.py --out <out-dir> --keys <keys-dir>
"""

import argparse
import sys
from pathlib import Path

import duckdb

import config
from export import day_files

EPOCH = "2000-01-01"
_OBX = {
    "msg_seq": "keep", "obr_set_id": "keep", "obx_set_id": "keep", "value_type": "keep",
    "obs_identifier": "keep", "obs_code": "keep", "obs_name": "keep", "sub_id": "keep",
    "channel": "keep", "units": "keep", "ref_range": "keep", "abn_flag": "keep",
    "result_status": "keep", "obs_datetime": "ts", "equipment_id": "device", "obs_site": "keep",
}
COLUMNS: dict[str, dict[str, str]] = {
    "msh": {
        "msg_seq": "keep", "msg_ctrl_id": "keep", "received_at": "tstz", "msg_ts": "tstz",
        "feed": "keep", "sending_app": "keep", "sending_facility": "keep",
        "receiving_app": "keep", "receiving_facility": "keep", "msg_timestamp": "ts",
        "msg_type": "keep", "processing_id": "keep", "version_id": "keep",
        "accept_ack_type": "keep", "app_ack_type": "keep", "msg_profile": "keep",
    },
    "pid": {
        "msg_seq": "keep", "patient_id_list": "patient", "patient_name": "drop",
        "dob": "drop", "admin_sex": "keep", "account_number": "account",
    },
    "pv1": {
        "msg_seq": "keep", "patient_class": "keep", "patient_location": "keep",
        "visit_number": "visit", "admit_datetime": "ts", "visit_indicator": "keep",
    },
    # Order numbers embed device EUI-64s and timestamps. One shared "order" map keeps
    # alarm lifecycle events linked by filler_order_num.
    "obr": {
        "msg_seq": "keep", "obr_set_id": "keep", "placer_order_num": "order",
        "filler_order_num": "order", "service_id": "keep", "obs_start_time": "ts",
        "obs_end_time": "ts", "parent_number": "order",
    },
    "numerics":  {**_OBX, "feed": "keep", "value": "keep"},
    "waveforms": {**_OBX, "samples": "keep", "value": "keep"},
    "signals":   {**_OBX, "value": "keep"},
    "alarms":    {**_OBX, "value": "keep"},
}
PREFIX = {"patient": "P", "account": "A", "visit": "V", "order": "O", "device": "D"}

# HL7 TS with optional fraction and offset. Offset-less values are read as UTC.
_HL7_TS = """CREATE MACRO hl7_ts(s) AS try_strptime(nullif(s, ''), [
    '%Y%m%d%H%M%S.%f%z', '%Y%m%d%H%M%S%z', '%Y%m%d%H%M%z', '%Y%m%d%H%M%S.%f', '%Y%m%d%H%M%S'])"""


def _check_columns(con: duckdb.DuckDBPyConnection) -> None:
    actual = con.execute(
        "SELECT table_name, list(column_name) FROM information_schema.columns "
        "WHERE table_schema = 's' GROUP BY ALL"
    ).fetchall()
    unknown = [f"{t}.{c}" for t, cols in actual for c in cols if c not in COLUMNS.get(t, {})]
    if unknown:
        sys.exit(f"columns without a de-identification rule (add them to COLUMNS): {unknown}")


def _build_keys(con: duckdb.DuckDBPyConnection, keys: Path) -> None:
    """Create bed_shift, msg_shift and ids tables, extending any saved keys."""
    for name, ddl in [("bed_shift", "patient_location VARCHAR, shift_days INTEGER"),
                      ("ids", "kind VARCHAR, original VARCHAR, pseudo VARCHAR")]:
        saved = keys / f"{name}.parquet"
        con.execute(f"CREATE TABLE {name} ({ddl})")
        if saved.exists():
            con.execute(f"INSERT INTO {name} SELECT * FROM '{saved.as_posix()}'")

    # Bed per message ('' = no bed), then a whole-day shift for each new bed
    con.execute("""
        CREATE TABLE msg_bed AS
        SELECT m.msg_seq, coalesce(p.bed, '') AS bed, min(coalesce(m.msg_ts, m.received_at)) AS ts
        FROM s.msh m
        LEFT JOIN (SELECT msg_seq, any_value(nullif(patient_location, '')) AS bed
                   FROM s.pv1 GROUP BY ALL) p USING (msg_seq)
        GROUP BY ALL""")
    con.execute(f"""
        INSERT INTO bed_shift
        SELECT bed, coalesce(date_diff('day', DATE '{EPOCH}', min(ts)::DATE), 0)
        FROM msg_bed WHERE bed NOT IN (SELECT patient_location FROM bed_shift)
        GROUP BY bed""")
    con.execute("""
        CREATE TABLE msg_shift AS
        SELECT msg_seq, shift_days
        FROM msg_bed JOIN bed_shift ON bed = patient_location""")

    # Pseudonyms: new values get the next numbers for their kind, in random order
    distinct = " UNION ".join(
        f"SELECT '{kind}' AS kind, nullif({col}, '') AS original FROM s.{table}"
        for table, cols in COLUMNS.items() for col, kind in cols.items() if kind in PREFIX
    )
    prefix = " ".join(f"WHEN '{k}' THEN '{p}'" for k, p in PREFIX.items())
    con.execute(f"""
        INSERT INTO ids
        SELECT kind, original,
               (CASE kind {prefix} END) || lpad((
                   coalesce((SELECT max(pseudo[2:]::INTEGER) FROM ids i WHERE i.kind = n.kind), 0)
                   + row_number() OVER (PARTITION BY kind ORDER BY r))::VARCHAR, 6, '0')
        FROM (SELECT *, random() AS r
              FROM (SELECT * FROM ({distinct}) WHERE original IS NOT NULL
                    EXCEPT SELECT kind, original FROM ids)) n""")

    keys.mkdir(parents=True, exist_ok=True)
    for name in ("bed_shift", "ids"):
        con.execute(f"COPY {name} TO '{(keys / f'{name}.parquet').as_posix()}' (FORMAT parquet)")


def _select(table: str) -> str:
    exprs = []
    for col, action in COLUMNS[table].items():
        if action == "keep":
            exprs.append(f"t.{col}")
        elif action == "ts":
            exprs.append(f"hl7_ts(t.{col}) - to_days(sh.shift_days) AS {col}")
        elif action == "tstz":
            exprs.append(f"t.{col} - to_days(sh.shift_days) AS {col}")
        elif action in PREFIX:
            exprs.append(f"(SELECT pseudo FROM ids WHERE kind = '{action}' AND original = nullif(t.{col}, '')) AS {col}")
    return (f"SELECT {', '.join(exprs)} FROM s.{table} t "
            f"LEFT JOIN msg_shift sh USING (msg_seq)")


def _report(con: duckdb.DuckDBPyConnection) -> None:
    n, no_pid, no_mrn, no_bed, beds = con.execute("""
        SELECT count(*),
               count(*) FILTER (WHERE msg_seq NOT IN (SELECT msg_seq FROM s.pid)),
               count(*) FILTER (WHERE msg_seq NOT IN (
                   SELECT msg_seq FROM s.pid WHERE nullif(patient_id_list, '') IS NOT NULL)),
               count(*) FILTER (WHERE bed = ''),
               count(DISTINCT bed)
        FROM msg_bed""").fetchall()[0]
    pct = lambda k: f"{k:,} ({100 * k / max(n, 1):.1f}%)"
    print(f"messages {n:,}  beds {beds:,}  no PID {pct(no_pid)}  no MRN {pct(no_mrn)}  no bed {pct(no_bed)}")

    bad = " UNION ALL ".join(
        f"SELECT '{t}.{c}', count(*) FROM s.{t} WHERE nullif({c}, '') IS NOT NULL AND hl7_ts({c}) IS NULL"
        for t, cols in COLUMNS.items() for c, a in cols.items() if a == "ts"
    )
    for col, k in con.execute(bad).fetchall():
        if k:
            print(f"WARNING {col}: {k:,} unparseable timestamps exported as NULL")


def _attach(con: duckdb.DuckDBPyConnection, dbs: list[Path]) -> None:
    """Attach every day file and expose each table as one view in schema s."""
    for i, db in enumerate(dbs):
        try:
            con.execute(f"ATTACH '{db.as_posix()}' AS f{i} (READ_ONLY)")
        except duckdb.IOException as e:
            sys.exit(f"cannot open {db} — is service.py still running?\n{e}")
    con.execute("CREATE SCHEMA s")
    for (table,) in con.execute("SELECT DISTINCT table_name FROM information_schema.tables "
                                "WHERE table_catalog LIKE 'f%'").fetchall():
        union = " UNION ALL BY NAME ".join(f"SELECT * FROM f{i}.{table}" for i in range(len(dbs)))
        con.execute(f"CREATE VIEW s.{table} AS {union}")


def export_deid(dbs: list[Path], out: Path, keys: Path) -> None:
    if out.resolve() == keys.resolve() or out.resolve() in keys.resolve().parents:
        sys.exit("--keys must not be inside --out: the keys re-identify the data")
    if not dbs:
        sys.exit("no day files to export")
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    _attach(con, dbs)
    con.execute(_HL7_TS)
    _check_columns(con)
    _build_keys(con, keys)
    _report(con)

    out.mkdir(parents=True, exist_ok=True)
    for (table,) in con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 's'").fetchall():
        dest = out / f"{table}.parquet"
        tmp = dest.with_suffix(".parquet.tmp")
        con.execute(f"COPY ({_select(table)}) TO '{tmp.as_posix()}' (FORMAT parquet, COMPRESSION zstd)")
        tmp.replace(dest)
        rows = con.execute(f"SELECT count(*) FROM '{dest.as_posix()}'").fetchall()[0][0]
        print(f"{table:<10} {rows:>14,} rows  {dest.stat().st_size / 1e6:>10,.1f} MB  {dest}")
    con.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", type=Path, nargs="+", help=f"day files (default: all in {config.STAGING_DIR})")
    ap.add_argument("--out", type=Path, default=Path("out/deid"), help="shareable output directory (default out/deid)")
    ap.add_argument("--keys", type=Path, default=Path("out/deid_keys"),
                    help="re-identification keys; keep on the server (default out/deid_keys)")
    args = ap.parse_args()
    export_deid(args.db or day_files(config.STAGING_DIR), args.out, args.keys)
