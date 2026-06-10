"""Multi-patient HL7 v2.6 mock generator for IHE PCD feeds.

Message generators and MLLP transport utilities — no CLI here.
Used by sender.py in fixed/stream modes.

Message types:
  DEC — ORU^R01  numeric vitals (ECG, IBP, SpO2, temp)
  ACM — ORU^R40  alarm lifecycle events
  WCM — ORU^R01  12-lead ECG waveforms (1440 samples @ 240 Hz)
"""

import datetime
import logging
import os
import random
import socket
import threading
import uuid
from dataclasses import dataclass

_MLLP_PORT = int(os.environ.get("PHYSIOMON_MLLP_PORT", "2575"))

log = logging.getLogger(__name__)

# ── Patient / device identity ─────────────────────────────────────────────────

@dataclass
class PatientDevice:
    patient_id: str
    bed:        str
    eui:        str
    model:      str = "B450"


def make_patients(n: int) -> list[PatientDevice]:
    return [
        PatientDevice(patient_id=str(1000 + i), bed=f"BED-{i:04d}", eui=f"3C1A{i:08X}")
        for i in range(n)
    ]

# ── Common MLLP info ─────────────────────────────────────────────────

# Spec-defined ports are DEC:2575, ACM:2576, WCM:2577.
# All feeds target MLLP_PORT (default 2575) until the aggregator is
# configured with separate ports per feed.
FEED_PORTS: dict[str, int] = {"DEC": _MLLP_PORT, "ACM": _MLLP_PORT, "WCM": _MLLP_PORT}

MLLP_START = b"\x0b"
MLLP_END   = b"\x1c\x0d"


@dataclass
class FeedConfig:
    name: str
    port: int


FEEDS: list[FeedConfig] = [FeedConfig(n, p) for n, p in FEED_PORTS.items()]

# ── Constants ─────────────────────────────────────────────────────────────────

TZ_OFFSET = "-0400"
SEG       = "\n"

IHE_PCD_001 = "IHE_PCD_001^IHE PCD^1.3.6.1.4.1.19376.1.6.1.1.1^ISO"
IHE_PCD_ACM = "IHE_PCD_ACM_001^IHE PCD^1.3.6.1.4.1.19376.1.6.1.4.1^ISO"

# ── Timestamp helpers ─────────────────────────────────────────────────────────

def _hl7_now() -> str:
    now = datetime.datetime.now()
    return now.strftime(f"%Y%m%d%H%M%S.{now.strftime('%f')[:4]}{TZ_OFFSET}")

def _hl7_ts_plain() -> str:
    return datetime.datetime.now().strftime(f"%Y%m%d%H%M%S.0000{TZ_OFFSET}")

def _obr_order_id(p: PatientDevice, feed: str) -> str:
    now = datetime.datetime.now()
    ts  = now.strftime("%Y%m%d%H%M%S") + f".{now.strftime('%f')[:4]}"
    return f"{p.eui}{ts}^{feed}^{p.eui}^EUI-64"

def _equip_id(p: PatientDevice, feed: str) -> str:
    return f"{p.bed}~{p.model}^{feed}^{p.eui}^EUI-64"

# ── Segment builders ──────────────────────────────────────────────────────────

def _msh(p: PatientDevice, feed: str,
         msg_type: str = "ORU^R01^ORU_R01",
         profile: str = IHE_PCD_001) -> str:
    return (
        f"MSH|^~\\&|{feed}||||{_hl7_now()}||{msg_type}|{uuid.uuid4()}"
        f"|P|2.6|||AL|NE|||||{profile}"
    )

def _pv1(p: PatientDevice) -> str:
    return f"PV1||I|{p.bed}|||||||||||||||||||||||||||||||||||||||||||||||||"

def _obr(p: PatientDevice, set_id: int, svc_code: str, svc_name: str,
         svc_sys: str, end_time: str, feed: str) -> str:
    oid = _obr_order_id(p, feed)
    return (
        f"OBR|{set_id}|{oid}|{oid}|{svc_code}^{svc_name}^{svc_sys}"
        f"|||{end_time}|||||||||||||||||||||||||||||||||||||"
    )

def _obx_struct(n: int, code: str, name: str, sys: str, sub: str) -> str:
    return f"OBX|{n}||{code}^{name}^{sys}|{sub}|||||||X|||||||||"

def _obx_nm(p: PatientDevice, n: int, code: str, name: str, sub: str,
            value: str, units: str, ref: str, abn: str, ts: str, feed: str) -> str:
    status = "R" if value else "X"
    return (
        f"OBX|{n}|NM|{code}^{name}^MDC|{sub}|{value}|{units}|{ref}|{abn}"
        f"|||{status}|||{ts}||||{_equip_id(p, feed)}||"
    )

def _obx_na(p: PatientDevice, n: int, code: str, name: str, sub: str,
            values: str, units: str, ts: str, feed: str) -> str:
    return f"OBX|{n}|NA|{code}^{name}^MDC|{sub}|{values}|{units}|||||R|||{ts}||||{_equip_id(p, feed)}||"

def _obx_nm_attr(n: int, code: str, name: str, sub: str, value: str, units: str = "") -> str:
    return f"OBX|{n}|NM|{code}^{name}^MDC|{sub}|{value}|{units}|||||R|||||||"

def _obx_nr_attr(n: int, code: str, name: str, sub: str, lo: str, hi: str) -> str:
    return f"OBX|{n}|NR|{code}^{name}^MDC|{sub}|{lo}^{hi}||||||R|||||||"

def _abn(v: float, lo: float, hi: float) -> str:
    if v < lo: return "L"
    if v > hi: return "H"
    return ""

# ── ACM alarm definitions ─────────────────────────────────────────────────────

@dataclass
class AlarmDef:
    alarm_code: str
    alarm_text: str
    priority:   str
    alert_type: str
    vmd_id:    int | None = None
    vmd_code:  str | None = None
    chan_id:   int | None = None
    chan_code:  str | None = None


ALARM_TYPES: list[AlarmDef] = [
    AlarmDef("0^MDCX_EVT_ALARM_MED^MDC",   "MULTIFOC PVCS",   "PL", "SP"),
    AlarmDef("0^MDCX_EVT_ALARM_MED^MDC",   "VNT ALARM",       "PM", "SP"),
    AlarmDef("0^MDCX_EVT_ALARM_HI^MDC",    "TACHY",           "PL", "SP"),
    AlarmDef("0^MDCX_EVT_ALARM_HI^MDC",    "BRADY",           "PL", "SP"),
    AlarmDef("0^MDCX_EVT_ALARM_HI^MDC",    "SPO2 LOW",        "PL", "SP"),
    AlarmDef("0^MDCX_EVT_ALARM_IMMED^MDC", "V-TACH",          "HI", "SP"),
    AlarmDef("199812^MDC_EVT_ECG_V_P_C^MDC",      "V P C",   "PN", "SP",
             vmd_id=5, vmd_code="69798^MDC_DEV_ECG_VMD^MDC"),
    AlarmDef("199818^MDC_EVT_ECG_V_P_C_PAIR^MDC", "COUPLET", "PN", "SP",
             vmd_id=5, vmd_code="69798^MDC_DEV_ECG_VMD^MDC"),
    AlarmDef("150392^MDC_EVT_SENSOR_DISCONN^MDC", "T2 SENSOR FAIL", "PN", "ST",
             vmd_id=26, vmd_code="69902^MDC_DEV_METER_TEMP_VMD^MDC",
             chan_id=2,  chan_code="69903^MDC_DEV_METER_TEMP_CHAN^MDC"),
    AlarmDef("150392^MDC_EVT_SENSOR_DISCONN^MDC", "IBP LINE DISCON", "PN", "ST",
             vmd_id=13, vmd_code="69854^MDC_DEV_METER_PRESS_BLD_VMD^MDC",
             chan_id=1,  chan_code="69855^MDC_DEV_METER_PRESS_BLD_CHAN^MDC"),
]

_MDS_CODE = "69965^MDC_DEV_MON_PHYSIO_MULTI_PARAM_MDS^MDC"


def _split_ce(ce: str) -> tuple[str, str, str]:
    parts = ce.split("^")
    return parts[0], parts[1], parts[2] if len(parts) > 2 else "MDC"

# ── WCM waveform constants ────────────────────────────────────────────────────

SAMPLE_RATE       = 240
BATCH_SECS        = 6
SAMPLES_PER_BATCH = SAMPLE_RATE * BATCH_SECS   # 1440

ECG_LEADS = [
    ("131329", "MDC_ECG_ELEC_POTL_I"),   ("131330", "MDC_ECG_ELEC_POTL_II"),
    ("131389", "MDC_ECG_ELEC_POTL_III"), ("131390", "MDC_ECG_ELEC_POTL_AVR"),
    ("131391", "MDC_ECG_ELEC_POTL_AVL"), ("131392", "MDC_ECG_ELEC_POTL_AVF"),
    ("131331", "MDC_ECG_ELEC_POTL_V1"),  ("131332", "MDC_ECG_ELEC_POTL_V2"),
    ("131333", "MDC_ECG_ELEC_POTL_V3"),  ("131334", "MDC_ECG_ELEC_POTL_V4"),
    ("131335", "MDC_ECG_ELEC_POTL_V5"),  ("131336", "MDC_ECG_ELEC_POTL_V6"),
]

DATA_INVALID_SENTINELS = [-32768, -32766, -32765, -32764, -32760,
                           -32759, -32758, -32757, -32756, -32755, -32754]
DATA_MISSING_SENTINELS = [-32767, -32763, -32762, -32761, -32753, -32752]


def _simulate_ecg(n: int, amplitude: int = 300) -> list[int]:
    samples = []
    for i in range(n):
        t   = i / SAMPLE_RATE
        hr  = 1.2
        val = int(amplitude * (
            0.1 * random.gauss(0, 0.3) +
            0.9 * (abs(((t * hr) % 1.0) - 0.5) < 0.05) * random.gauss(0, 1)
        ))
        samples.append(max(-32000, min(32000, val)))
    return samples

# ── Message generators ────────────────────────────────────────────────────────

def generate_dec(p: PatientDevice) -> str:
    ts   = _hl7_ts_plain()
    feed = "DEC"
    n    = 0

    def nx() -> int:
        nonlocal n; n += 1; return n

    hr       = round(random.gauss(75,  8),  1)
    vpc      = round(max(0, random.gauss(2, 2)), 1)
    spo2     = round(min(100, random.gauss(97, 1)), 1)
    pr       = round(random.gauss(75,  8),  1)
    sqi      = round(min(100, random.gauss(99, 1)), 4)
    st       = {k: round(random.gauss(0, 20), 0)
                for k in ["I","II","III","V1","V2","V3","V4","V5","V6","AVF","AVL","AVR"]}
    ibp_sys  = round(random.gauss(118, 12), 1)
    ibp_dia  = round(random.gauss(70,   8), 1)
    ibp_mean = round(random.gauss(90,   9), 1)
    ibp_hr   = round(random.gauss(75,   8), 1)
    temp1    = round(random.gauss(37.0, 0.3), 1)

    segs = [
        _msh(p, feed),
        _pv1(p),
        _obr(p, 1, "182777000", "monitoring of patient", "SCT", ts, feed),
        _obx_struct(nx(), "69965", "MDC_DEV_MON_PHYSIO_MULTI_PARAM_MDS", "MDC", "1.0.0.0"),
        _obx_struct(nx(), "69798", "MDC_DEV_ECG_VMD", "MDC", "1.5.0.0"),
        _obx_nm(p, nx(), "147842", "MDC_ECG_HEART_RATE",  "1.5.0.1",  str(hr),        "{beat}/min^{beat}/min^UCUM", "50-100",    _abn(hr,  50, 100), ts, feed),
        _obx_nm(p, nx(), "148066", "MDC_ECG_V_P_C_RATE",  "1.5.0.2",  str(vpc),       "{beat}/min^{beat}/min^UCUM", "",          "",                 ts, feed),
        _obx_nm(p, nx(), "131841", "MDC_ECG_AMPL_ST_I",   "1.5.0.3",  str(st["I"]),   "uV^uV^UCUM", "", "", ts, feed),
        _obx_nm(p, nx(), "131842", "MDC_ECG_AMPL_ST_II",  "1.5.0.4",  str(st["II"]),  "uV^uV^UCUM", "", "", ts, feed),
        _obx_nm(p, nx(), "131901", "MDC_ECG_AMPL_ST_III", "1.5.0.5",  str(st["III"]), "uV^uV^UCUM", "", "", ts, feed),
        _obx_nm(p, nx(), "131843", "MDC_ECG_AMPL_ST_V1",  "1.5.0.6",  str(st["V1"]),  "uV^uV^UCUM", "", "", ts, feed),
        _obx_nm(p, nx(), "131844", "MDC_ECG_AMPL_ST_V2",  "1.5.0.7",  str(st["V2"]),  "uV^uV^UCUM", "", "", ts, feed),
        _obx_nm(p, nx(), "131845", "MDC_ECG_AMPL_ST_V3",  "1.5.0.8",  str(st["V3"]),  "uV^uV^UCUM", "", "", ts, feed),
        _obx_nm(p, nx(), "131846", "MDC_ECG_AMPL_ST_V4",  "1.5.0.9",  str(st["V4"]),  "uV^uV^UCUM", "", "", ts, feed),
        _obx_nm(p, nx(), "131847", "MDC_ECG_AMPL_ST_V5",  "1.5.0.10", str(st["V5"]),  "uV^uV^UCUM", "", "", ts, feed),
        _obx_nm(p, nx(), "131848", "MDC_ECG_AMPL_ST_V6",  "1.5.0.11", str(st["V6"]),  "uV^uV^UCUM", "", "", ts, feed),
        _obx_nm(p, nx(), "131904", "MDC_ECG_AMPL_ST_AVF", "1.5.0.12", str(st["AVF"]), "uV^uV^UCUM", "", "", ts, feed),
        _obx_nm(p, nx(), "131903", "MDC_ECG_AMPL_ST_AVL", "1.5.0.13", str(st["AVL"]), "uV^uV^UCUM", "", "", ts, feed),
        _obx_nm(p, nx(), "131902", "MDC_ECG_AMPL_ST_AVR", "1.5.0.14", str(st["AVR"]), "uV^uV^UCUM", "", "", ts, feed),
        _obx_struct(nx(), "69854", "MDC_DEV_METER_PRESS_BLD_VMD",  "MDC", "1.13.0.0"),
        _obx_struct(nx(), "69855", "MDC_DEV_METER_PRESS_BLD_CHAN", "MDC", "1.13.1.0"),
    ]
    for code, name, val, units, ref, lo, hi in [
        ("150034", "MDC_PRESS_BLD_ART_DIA",  ibp_dia,  "mm[Hg]^mm[Hg]^UCUM", "60-90",  60,  90),
        ("150035", "MDC_PRESS_BLD_ART_MEAN", ibp_mean, "mm[Hg]^mm[Hg]^UCUM", "70-105", 70, 105),
        ("149522", "MDC_BLD_PULS_RATE_INV",  ibp_hr,   "{beat}/min^{beat}/min^UCUM", "50-100", 50, 100),
        ("150033", "MDC_PRESS_BLD_ART_SYS",  ibp_sys,  "mm[Hg]^mm[Hg]^UCUM", "90-140", 90, 140),
    ]:
        i = nx()
        segs.append(_obx_nm(p, i, code, name, f"1.13.1.{i}", str(val), units, ref, _abn(val, lo, hi), ts, feed))

    segs += [
        _obx_struct(nx(), "69642", "MDC_DEV_ANALY_SAT_O2_VMD",  "MDC", "1.22.0.0"),
        _obx_struct(nx(), "69643", "MDC_DEV_ANALY_SAT_O2_CHAN", "MDC", "1.22.1.0"),
    ]
    for code, name, val, units, ref, lo, hi in [
        ("149530", "MDC_PULS_OXIM_PULS_RATE",      pr,   "{beat}/min^{beat}/min^UCUM", "50-100", 50, 100),
        ("150456", "MDC_PULS_OXIM_SAT_O2",         spo2, "%^%^UCUM",                  "95-100", 95, 100),
        ("160324", "MDC_SPO2_SIGNAL_QUALITY_INDEX", sqi,  "%^%^UCUM",                  "",       0,  100),
    ]:
        i = nx()
        segs.append(_obx_nm(p, i, code, name, f"1.22.1.{i}", str(val), units, ref, _abn(val, lo, hi), ts, feed))

    segs += [
        _obx_struct(nx(), "69902", "MDC_DEV_METER_TEMP_VMD",  "MDC", "1.26.0.0"),
        _obx_struct(nx(), "69903", "MDC_DEV_METER_TEMP_CHAN", "MDC", "1.26.1.0"),
    ]
    i = nx()
    segs.append(_obx_nm(p, i, "150344", "MDC_TEMP", f"1.26.1.{i}", str(temp1),
                        "Cel^Cel^UCUM", "36.0-38.0", _abn(temp1, 36.0, 38.0), ts, feed))

    return SEG.join(segs) + SEG


def generate_acm(p: PatientDevice) -> str:
    alarm      = random.choice(ALARM_TYPES)
    feed       = "ACM"
    ts         = _hl7_ts_plain()
    now        = _hl7_now()
    order_uuid = str(uuid.uuid4())
    eq         = _equip_id(p, feed)

    vmd  = alarm.vmd_id or 0
    chan = alarm.chan_id or 0
    base = f"1.{vmd}.{chan}.1"
    src  = alarm.vmd_code if alarm.vmd_code else _MDS_CODE

    segs = [
        _msh(p, feed, "ORU^R40^ORU_R40", IHE_PCD_ACM),
        _pv1(p),
        (f"OBR|1|{order_uuid}^ACM^{p.eui}^EUI-64"
         f"|{order_uuid}^ACM^{p.eui}^EUI-64"
         f"|196616^MDC_EVT_ALARM^MDC|||{ts}|||||||||||||||||||||||||||||||||||||||||"),
        _obx_struct(1, "69965", "MDC_DEV_MON_PHYSIO_MULTI_PARAM_MDS", "MDC", "1.0.0.0"),
    ]

    n = 1
    if alarm.vmd_code:
        n += 1
        segs.append(_obx_struct(n, *_split_ce(alarm.vmd_code), f"1.{vmd}.0.0"))
        if alarm.chan_code:
            n += 1
            segs.append(_obx_struct(n, *_split_ce(alarm.chan_code), f"1.{vmd}.{chan}.0"))

    n += 1
    segs.append(
        f"OBX|{n}|ST|{alarm.alarm_code}|{base}.1|{alarm.alarm_text}"
        f"|||A~{alarm.priority}~{alarm.alert_type}|||R|||{ts}||||{eq}||"
    )
    segs.append(f"OBX|{n+1}|ST|68480^MDC_ATTR_ALERT_SOURCE^MDC|{base}.2|{src}||||||R|||{now}||||||")
    segs.append(f"OBX|{n+2}|ST|68481^MDC_ATTR_EVENT_PHASE^MDC|{base}.3|start||||||R|||||||||")
    segs.append(f"OBX|{n+3}|ST|68482^MDC_ATTR_ALARM_STATE^MDC|{base}.4|active||||||R|||||||||")
    segs.append(f"OBX|{n+4}|ST|68483^MDC_ATTR_ALARM_INACTIVATION_STATE^MDC|{base}.5|enabled||||||X|||||||||")
    segs.append(f"OBX|{n+5}|ST|68484^MDC_ATTR_ALARM_PRIORITY^MDC|{base}.6|{alarm.priority}||||||R|||||||||")
    segs.append(f"PID|{p.patient_id}")
    segs.append(f"OBX|{n+6}|ST|68485^MDC_ATTR_ALERT_TYPE^MDC|{base}.7|{alarm.alert_type}||||||R|||||||||")

    return SEG.join(segs) + SEG


def generate_wcm(p: PatientDevice) -> str:
    feed     = "WCM"
    end_ts   = _hl7_ts_plain()
    end_dt   = datetime.datetime.now()
    start_ts = (end_dt - datetime.timedelta(seconds=BATCH_SECS)).strftime(
        f"%Y%m%d%H%M%S.0000{TZ_OFFSET}"
    )

    segs = [_msh(p, feed), _pv1(p)]
    n    = 0

    def nx() -> int:
        nonlocal n; n += 1; return n

    hr       = round(random.gauss(75,  8),  1)
    spo2     = round(min(100, random.gauss(97, 1)), 1)
    ibp_sys  = round(random.gauss(118, 12), 1)
    ibp_dia  = round(random.gauss(70,   8), 1)
    ibp_mean = round(random.gauss(90,   9), 1)
    ibp_hr   = round(random.gauss(75,   8), 1)

    segs.append(_obr(p, 1, "182777000", "monitoring of patient", "SCT", end_ts, feed))
    segs.append(_obx_struct(nx(), "69965", "MDC_DEV_MON_PHYSIO_MULTI_PARAM_MDS", "MDC", "1.0.0.0"))
    segs.append(_obx_struct(nx(), "69798", "MDC_DEV_ECG_VMD",                    "MDC", "1.5.0.0"))
    segs.append(_obx_nm(p, nx(), "147842", "MDC_ECG_HEART_RATE", "1.5.0.1", str(hr),
                        "{beat}/min^{beat}/min^UCUM", "50-100", _abn(hr, 50, 100), end_ts, feed))
    segs.append(_obx_struct(nx(), "69854", "MDC_DEV_METER_PRESS_BLD_VMD",  "MDC", "1.13.0.0"))
    segs.append(_obx_struct(nx(), "69855", "MDC_DEV_METER_PRESS_BLD_CHAN", "MDC", "1.13.1.0"))
    for code, name, val, sub in [
        ("150034", "MDC_PRESS_BLD_ART_DIA",  ibp_dia,  "1.13.1.3"),
        ("150035", "MDC_PRESS_BLD_ART_MEAN", ibp_mean, "1.13.1.4"),
        ("149522", "MDC_BLD_PULS_RATE_INV",  ibp_hr,   "1.13.1.5"),
        ("150033", "MDC_PRESS_BLD_ART_SYS",  ibp_sys,  "1.13.1.6"),
    ]:
        segs.append(_obx_nm(p, nx(), code, name, sub, str(val), "mm[Hg]^mm[Hg]^UCUM", "", "", end_ts, feed))
    segs.append(_obx_struct(nx(), "69642", "MDC_DEV_ANALY_SAT_O2_VMD",  "MDC", "1.22.0.0"))
    segs.append(_obx_struct(nx(), "69643", "MDC_DEV_ANALY_SAT_O2_CHAN", "MDC", "1.22.1.0"))
    segs.append(_obx_nm(p, nx(), "149530", "MDC_PULS_OXIM_PULS_RATE", "1.22.1.11",
                        str(round(random.gauss(75, 8), 1)), "{beat}/min^{beat}/min^UCUM", "", "", end_ts, feed))
    segs.append(_obx_nm(p, nx(), "150456", "MDC_PULS_OXIM_SAT_O2",    "1.22.1.12",
                        str(spo2), "%^%^UCUM", "", "", end_ts, feed))
    segs.append(_obx_nm(p, nx(), "160324", "MDC_SPO2_SIGNAL_QUALITY_INDEX", "1.22.1.13",
                        "99.89999", "%^%^UCUM", "", "", end_ts, feed))

    for wf_seq, (lead_code, lead_name) in enumerate(ECG_LEADS, start=2):
        wave_str = "^".join(str(s) for s in _simulate_ecg(SAMPLES_PER_BATCH))
        segs.append(_obr(p, wf_seq, "69121", "MDC_OBS_WAVE_CTS", "MDC", end_ts, feed))
        segs.append(_obx_struct(1, "69965", "MDC_DEV_MON_PHYSIO_MULTI_PARAM_MDS", "MDC", "1.0.0.0"))
        segs.append(_obx_struct(2, "69798", "MDC_DEV_ECG_VMD",                    "MDC", "1.5.0.0"))
        segs.append(_obx_na(p, 3, lead_code, lead_name, "1.5.0.1", wave_str, "uV^uV^UCUM", start_ts, feed))
        segs.append(_obx_nm_attr(4, "68320", "MDC_ATTR_SAMPLE_RATE", "1.5.0.1.1", str(SAMPLE_RATE), "s^s^UCUM"))
        segs.append(_obx_nr_attr(5, "68323", "MDC_ATTR_DATA_RANGE",  "1.5.0.1.2", "-32751", "32767"))
        for i, sv in enumerate(DATA_INVALID_SENTINELS, start=1):
            segs.append(_obx_nm_attr(5+i, "197376", "MDC_EVT_DATA_INVALID", f"1.5.0.1.2.{i}", str(sv)))
        offset = len(DATA_INVALID_SENTINELS)
        for i, sv in enumerate(DATA_MISSING_SENTINELS, start=1):
            segs.append(_obx_nm_attr(5+offset+i, "197378", "MDC_EVT_DATA_MISSING", f"1.5.0.1.2.{offset+i}", str(sv)))
        segs.append(_obx_nm_attr(5+offset+len(DATA_MISSING_SENTINELS)+1,
                                 "67945", "MDC_ATTR_SA_MSMT_RES", "1.5.0.1.3", "2.44"))

    return SEG.join(segs) + SEG

# ── MLLP transport ────────────────────────────────────────────────────────────


def run_feed(
    p: PatientDevice,
    feed_name: str,
    port: int,
    generator_fn,
    interval: float,
    host: str,
    count: int | None,
    stop_event: threading.Event,
) -> None:
    """Maintain a persistent MLLP connection and send messages at interval.

    No ACK is expected (receiver runs with --no-ack).
    Feed exits when *count* is reached or *stop_event* fires.
    """
    flog = logging.getLogger(f"gen.{feed_name}")
    sent = 0
    errors = 0

    while not stop_event.is_set() and (count is None or sent < count):
        try:
            flog.info("connecting  %s:%d  patient=%s", host, port, p.patient_id)
            with socket.create_connection((host, port), timeout=10) as conn:
                flog.info("connected   %s:%d", host, port)
                while not stop_event.is_set() and (count is None or sent < count):
                    msg = generator_fn(p)
                    try:
                        conn.sendall(MLLP_START + msg.encode("utf-8") + MLLP_END)
                        sent += 1
                        flog.debug("sent #%d", sent)
                        stop_event.wait(timeout=interval)
                    except OSError as exc:
                        flog.error("send error: %s", exc)
                        errors += 1
                        break
        except OSError as exc:
            flog.warning("cannot connect %s:%d: %s — retrying", host, port, exc)
            errors += 1
            stop_event.wait(timeout=5)

    flog.info("stopped  sent=%d  errors=%d", sent, errors)


_FEED_INTERVALS:   dict[str, float]    = {"DEC": 6.0, "ACM": 20.0, "WCM": 6.0}
_FEED_GENERATORS:  dict[str, callable] = {"DEC": generate_dec, "ACM": generate_acm, "WCM": generate_wcm}
