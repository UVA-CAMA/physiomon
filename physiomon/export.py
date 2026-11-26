"""Export every table of each staging day file to zstd parquet: <out>/<day>/<table>.parquet.

DuckDB is single-writer, so stop service.py first. Messages wait in NATS and
are ingested when the service restarts.

    python export.py                                    # every day in config.STAGING_DIR -> out/export/
    python export.py --out <out-dir>
    python export.py --db out/staging/20260110.duckdb --out <out-dir>
"""

import argparse
import sys
from pathlib import Path

import duckdb

import config


def day_files(staging_dir: str) -> list[Path]:
    """Every day file, including late/ ones (exported as late_YYYYMMDD)."""
    return sorted(Path(staging_dir).glob("*.duckdb")) + sorted(Path(staging_dir).glob("late/*.duckdb"))


def export(db: str, out: Path) -> None:
    try:
        con = duckdb.connect(db, read_only=True)
    except duckdb.IOException as e:
        sys.exit(f"cannot open {db} — is service.py still running?\n{e}")

    out.mkdir(parents=True, exist_ok=True)
    for (table,) in con.execute("SHOW TABLES").fetchall():
        dest = out / f"{table}.parquet"
        tmp = dest.with_suffix(".parquet.tmp")
        rows = con.execute(f"SELECT count(*) FROM {table}").fetchall()[0][0]
        con.execute(f"COPY {table} TO '{tmp.as_posix()}' (FORMAT parquet, COMPRESSION zstd)")
        tmp.replace(dest)  # atomic: readers never see a partial file
        print(f"{table:<10} {rows:>14,} rows  {dest.stat().st_size / 1e6:>10,.1f} MB  {dest}")
    con.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", nargs="+", help=f"day files (default: all in {config.STAGING_DIR})")
    ap.add_argument("--out", type=Path, default=Path("out/export"), help="output directory (default out/export)")
    args = ap.parse_args()
    for db in args.db or day_files(config.STAGING_DIR):
        db = Path(db)
        export(str(db), args.out / f"{'late_' if db.parent.name == 'late' else ''}{db.stem}")
