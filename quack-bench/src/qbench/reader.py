"""Reader process: loop a fixed query set against the writer over Quack, recording latency.

    python -m qbench.reader --seconds 30 --out reader0.json

Queries run server-side via quack_query, so they see every day file the writer has
attached. The freshness query returns how old the newest visible row is.
"""

import argparse
import json
import time
from pathlib import Path

import duckdb

from qbench.writer import TOKEN

QUERIES = {
    "count":        "SELECT count(*) FROM numerics_all",
    "bed_numerics": "SELECT obs_code, avg(value) FROM numerics_all WHERE bed = 'UNIT0^BED3' GROUP BY 1",
    "all_numerics": "SELECT bed, obs_code, avg(value), max(value) FROM numerics_all GROUP BY ALL",
    "bed_waves":    "SELECT count(*), sum(octet_length(samples)) FROM waves_all WHERE bed = 'UNIT0^BED3'",
    "freshness":    "SELECT epoch(now() - max(written_at)) FROM numerics_all",
}


def run(uri: str, seconds: float, out: Path) -> None:
    con = duckdb.connect()
    con.execute("LOAD quack")
    latencies: dict[str, list[float]] = {q: [] for q in QUERIES}
    staleness: list[float] = []
    errors: list[str] = []
    end = time.perf_counter() + seconds
    while time.perf_counter() < end:
        for name, sql in QUERIES.items():
            t0 = time.perf_counter()
            try:
                rows = con.execute("SELECT * FROM quack_query(?, ?, token := ?)", [uri, sql, TOKEN]).fetchall()
            except duckdb.Error as e:
                errors.append(f"{name}: {e}"[:300])
                continue
            latencies[name].append(time.perf_counter() - t0)
            if name == "freshness" and rows and rows[0][0] is not None:
                staleness.append(rows[0][0])
    out.write_text(json.dumps({"latencies": latencies, "staleness": staleness, "errors": errors}))


def probe_attach(uri: str) -> str:
    """Can a client ATTACH the server and see the cross-day views directly?"""
    con = duckdb.connect()
    con.execute("LOAD quack")
    try:
        con.execute(f"ATTACH '{uri}' AS r (TOKEN '{TOKEN}')")
        return f"ok: {con.execute('SELECT count(*) FROM r.numerics_all').fetchall()[0][0]:,} rows"
    except duckdb.Error as e:
        return f"failed: {str(e).splitlines()[0]}"


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--uri", default="quack:localhost:9494")
    ap.add_argument("--seconds", type=float, default=30)
    ap.add_argument("--out", type=Path, required=True)
    a = ap.parse_args()
    run(a.uri, a.seconds, a.out)
