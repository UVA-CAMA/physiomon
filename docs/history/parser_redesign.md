# Parser Redesign — Decisions

Resolved design decisions for the `parse_vectorized` / `parse_batch` redesign.
See `decisions.md` for higher-level architectural decisions (storage format,
write strategy, HSDI catalog integration).

---

## Output: three parquet files

| File | Content | OBX value type |
|------|---------|---------------|
| `numerics.parquet` | One row per numeric OBX (DEC + WCM numerics) | `NM` — scalar f64 |
| `waveforms.parquet` | One row per waveform OBX (WCM + ACM snippets) | `NA` — list[i16] |
| `alarms.parquet` | One row per alarm event (ACM), pivoted | `ST` + alarm attributes |

**Why separated:**

- **Numerics vs waveforms**: value column type is incompatible — scalar `f64` vs `list[i16]`. Can't coexist in one typed parquet column.
- **Numerics vs alarms**: an alarm "event" is a group of OBX rows (identity, measured value, phase, state, priority, threshold) that pivot into one output row. Alarm attribute rows mixed into a numerics file would be structurally incoherent.
- **Volume**: waveforms are ~99% of raw bytes. Separating them keeps numerics queries fast.

Both `VitalsFiles` and `WaveformFiles` tables in the existing HSDI SQL Server catalog
already reflect this split.

---

## Format: long, not wide

One row per observation (OBX), not one row per timestamp with one column per signal.

**Why not wide:**

- Wide format requires a stable shared timestamp axis. Signals in a message do
  share a timestamp (OBX-14 repeats the OBR-7 value for all OBX in that group),
  so this is not the blocking issue. The blocking issue is:
- The same MDC code can appear on multiple channels (e.g. `MDC_TEMP` on channels
  1.26.1 and 1.26.2 for two temperature probes). Wide format would need column
  names like `MDC_TEMP_1` / `MDC_TEMP_2` — schema varies by patient and device
  configuration, requiring schema merge when combining files across patients.
- Long format handles variable channel counts with no schema change: new channel =
  new rows.

---

## Signal identity key

**`obs_code + channel`** — not the HSDI `vtKey`.

- `obs_code` = OBX-3.0 (MDC numeric code, e.g. `147842`)
- `channel` = OBX-4 sub_id position 3 (VMD channel number, e.g. `1` for temp probe 1)

The sub_id format is `MDS.VMD.Channel.Metric` (always four positions for metric OBX).
Channel is the correct and stable disambiguator for multiple instances of the same
measurement type (two arterial lines, two temperature probes). OBX-20 `obs_site`
(e.g. `artery`, `pulmonary-artery`, `unknown`) is supplementary and unreliable as
a key — it can be `unknown` for multiple distinct channels.

This fixes a known gap in the existing HSDI pipeline: the GE SDK index parameter
was not captured, so temperature channel 1 and channel 2 were ambiguous. The HL7
`channel` field corrects this.

---

## Schemas

### `numerics.parquet`

| Column | Source | Type | Notes |
|--------|--------|------|-------|
| `msg_ctrl_id` | MSH-10 | str | UUID; dedup key |
| `msg_timestamp` | MSH-7 | str | Message send datetime |
| `bed_location` | PV1-3 | str | Room/bed/facility composite |
| `pid_patient_id` | PID-3 | str | **Null until confirmed** — PID stripped from sample files |
| `obs_code` | OBX-3.0 | str | MDC numeric code |
| `obs_name` | OBX-3.1 | str | MDC term (e.g. `MDC_ECG_HEART_RATE`) |
| `channel` | OBX-4 sub_id[2] | i32 | VMD channel number; disambiguates same-MDC-code instances |
| `sub_id` | OBX-4 | str | Full hierarchical path (preserved for reference) |
| `uom` | OBX-6.0 | str | UCUM unit code |
| `value` | OBX-5 | f64 | Cast from string |
| `abn_flag` | OBX-8 | str | `H`, `L`, etc. |
| `result_status` | OBX-11 | str | `R` (result), `X` (unavailable) |
| `obs_datetime` | OBX-14 | str | Observation timestamp |
| `obs_site` | OBX-20 | str | Anatomical label; supplementary only |
| `equipment_id` | OBX-18 | str | Device EUI identifier |

### `waveforms.parquet`

Only NA-type OBX rows. The NM/NR metadata rows for each waveform channel (sample
rate, scale factor, data range, sentinels) are NM/NR type and route to
`numerics.parquet`, tagged with the same `obr_service_id` and a `sub_id` one level
deeper than the waveform row (waveform at `1.5.0.1`, sample rate at `1.5.0.1.1`,
scale factor at `1.5.0.1.3`). Consumers find them by querying `numerics` for the
same `(msg_ctrl_id, obr_set_id)` with the relevant `obs_code`.

| Column | Source | Type | Notes |
|--------|--------|------|-------|
| `msg_ctrl_id` | MSH-10 | str | |
| `msg_timestamp` | MSH-7 | str | |
| `bed_location` | PV1-3 | str | |
| `pid_patient_id` | PID-3 | str | **Null until confirmed** |
| `obs_code` | OBX-3.0 | str | MDC lead code (e.g. `131329` = ECG lead I) |
| `obs_name` | OBX-3.1 | str | MDC term (e.g. `MDC_ECG_ELEC_POTL_I`) |
| `channel` | OBX-4 sub_id[2] | i32 | VMD channel number |
| `sub_id` | OBX-4 | str | Full hierarchical path |
| `uom` | OBX-6.0 | str | `uV` |
| `obs_start_time` | OBR-7 | str | Block start (6-second window) |
| `obs_end_time` | OBR-8 | str | Block end |
| `obr_service_id` | OBR-4 | str | `69121^MDC_OBS_WAVE_CTS^MDC` or `69122^MDC_OBS_WAVE_NONCTS^MDC` |
| `values` | OBX-5 | list[i16] | `^`-split and cast; 1440 integers per 6-second block |

**Sample encoding:** observed aggregator output sends all 1440 samples as individual
`^`-delimited integers — no run-length compression in practice. Decoder is a plain
Polars `str.split("^").list.eval(pl.element().cast(pl.Int16, strict=False))`.

### `alarms.parquet`

ST-type OBX rows from ACM (`ORU^R40`) messages — one row per OBX, not pivoted.
An alarm event is a group of rows sharing `(msg_ctrl_id, obr_set_id)`: the alarm
identity row, event phase, alarm state, priority, alert type, etc. are all separate
rows with distinct `obs_code` values. The triggering measured value (NM-type) lands
in `numerics.parquet` tagged with the alarm `obr_service_id`. Consumers pivot or
filter at query time using `(msg_ctrl_id, obr_set_id)` as the event group key.

Same schema as `numerics.parquet` — the columns are identical. The routing is by
`value_type`: ST rows go here, NM/NR rows go to numerics, NA rows go to waveforms.
`obr_service_id` will be `196616^MDC_EVT_ALARM^MDC` for alarm rows, which
distinguishes them from numeric vital sign rows in any cross-file query.

---

## Patient identity: optional enrichment, not a key

Files and directory paths are keyed by **location (unit + bed) + date**, not by
patient. PV1-3 (bed location) is always present; PID-3 (MRN) arrives only when
the device has an ADT match, which is not guaranteed. The system must function
correctly with no patient identity at all.

`pid_patient_id` and `pid_mrn` columns in all three schemas are always nullable.
When PID is present in a message, parse and store it. When absent, leave null.
No placeholder patient rows (`UNKNOWN_UNIT_BED` style) are created.

**Pending:** which PID-3 component carries MRN in production messages, and which
carries the internal patient key. This affects only how `pid_patient_id` and
`pid_mrn` are populated — it has no effect on the file hierarchy or signal schema.
Confirm from a live production message before implementing PID parsing.

---

## What changes from the current implementation

The parser's job is faithful representation: segments to rows, context
forward-filled, no obs_code-specific logic, no pivoting, no pre-joining.

**`parse_vectorized` — two additions:**

1. **`channel` column** — extract sub_id position 3 (0-indexed) from OBX-4.
   The sub_id format is `MDS.VMD.Channel.Metric`; channel is `split(".")[2]`
   cast to `i32`. Null for rows where sub_id has fewer than 3 components (rare
   metadata rows).

2. **PID parsing** — parse PID-3 into `pid_patient_id` (nullable str). Which
   component carries MRN vs. internal key is TBD pending a live production
   message. Leave null until confirmed.

**`cli.py` `write_results` — NA value decode:**

The existing `value_type` routing (NM/NR → numerics, NA → waveforms, ST →
alarms) is already in place and does not change. Add one transform to the
waveforms branch before writing:

```python
waveforms = waveforms.with_columns(
    pl.col("value")
    .str.split("^")
    .list.eval(pl.element().cast(pl.Int16, strict=False))
    .alias("values")
).drop("value")
```

No RLE decoding needed — aggregator output sends all 1440 samples as plain
`^`-delimited integers.

**Nothing else changes.** Alarm rows are flat ST rows (same schema as numerics);
consumers pivot at query time using `(msg_ctrl_id, obr_set_id)` as the event
group key. Sample rate, scale factor, and other NM metadata rows for waveform
channels route to numerics.parquet naturally via value_type; consumers find
them by querying numerics for the same `(msg_ctrl_id, obr_set_id)` and the
relevant `obs_code`.
