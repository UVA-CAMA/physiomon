"""Generate mock HL7 example files for physiosim/examples/.

Produces case1.hl7, case2.hl7 and matching case*.head.hl7 files containing
synthetic patient data in the same CSV format as real case files.

Run from repo root or physiosim/:
    python physiosim/make_examples.py
"""

import csv
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from generator import generate_dec, generate_acm, generate_wcm, make_patients

EXAMPLES = Path(__file__).parent / "examples"

# Match the real file distribution: mostly WCM, some DEC, few ACM
_CASES = [
    {"dec": 9,  "acm": 6, "wcm": 85},
    {"dec": 7,  "acm": 4, "wcm": 89},
]


def _make_rows(patient_idx: int, dec: int, acm: int, wcm: int) -> list[dict]:
    patient = make_patients(patient_idx + 1)[patient_idx]
    base_time = datetime(2024, 1, 1, 8, 0, 0)

    entries = (
        [("DEC", generate_dec)] * dec +
        [("ACM", generate_acm)] * acm +
        [("WCM", generate_wcm)] * wcm
    )
    random.shuffle(entries)

    rows = []
    t = base_time
    for msg_type, gen in entries:
        rows.append({
            "PIDID":        patient_idx + 1,
            "MESSAGEDATE":  t.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "MESSAGETYPE":  msg_type,
            "BEDLOCATION":  patient.bed,
            "HL7STRIPPED":  gen(patient),
        })
        t += timedelta(seconds=6)
    return rows


def _write(path: Path, rows: list[dict]) -> None:
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["PIDID", "MESSAGEDATE", "MESSAGETYPE", "BEDLOCATION", "HL7STRIPPED"])
        w.writeheader()
        w.writerows(rows)
    print(f"  {path.name}  ({len(rows)} rows)")


def main() -> None:
    random.seed(42)
    EXAMPLES.mkdir(exist_ok=True)
    print(f"writing to {EXAMPLES}/")

    for i, counts in enumerate(_CASES):
        rows = _make_rows(i, **counts)
        _write(EXAMPLES / f"case{i+1}.hl7",      rows)
        _write(EXAMPLES / f"case{i+1}.head.hl7", [r for r in rows if r["MESSAGETYPE"] == "WCM"][:5])


if __name__ == "__main__":
    main()
