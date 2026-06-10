# IHE PCD Aggregator HL7 Message Schema

This document describes the message structure and parsing contract for HL7 messages
produced by an IHE PCD-compliant aggregator. It covers all three feed types:
WFM/WCM (waveform), DEC (numeric only), and ACM (alarms).

---

## Message Type and Envelope

All feed messages are HL7 v2.6 unsolicited observation result messages, except
where noted below. The WCM/WFM and DEC feeds use `ORU^R01^ORU_R01` under the IHE PCD
profile `IHE_PCD_001` (OID `1.3.6.1.4.1.19376.1.6.1.1.1`). The ACM feed uses
`ORU^R40^ORU_R40` under the IHE PCD ACM profile `IHE_PCD_ACM_001` (OID
`1.3.6.1.4.1.19376.1.6.1.4.1`).

**MSH key fields (all feeds):**

| Field | Notes |
|---|---|
| MSH-3 | Sending application (e.g. `WFM`) |
| MSH-7 | Message timestamp, format `YYYYMMDDHHMMSS.mmmmZZZZ` |
| MSH-9 | `ORU^R01^ORU_R01` (DEC/WCM) or `ORU^R40^ORU_R40` (ACM) |
| MSH-10 | Message control ID (UUID in observed samples) |
| MSH-11 | `P` (production) |
| MSH-12 | `2.6` |
| MSH-15 | `AL` (accept ACK always) |
| MSH-16 | `NE` (application ACK never) |
| MSH-21 | Profile ID component — IHE PCD OID |

---

## Patient and Visit Segments

**PID** contains patient identity. In a live feed: patient ID (PID-3), name (PID-5),
date of birth (PID-7), sex (PID-8), and account number (PID-18). In the sample
files patient data is anonymized.

**PV1** contains visit context: patient class (PV1-2), bed location (PV1-3), and
admit date/time (PV1-44).

> **Implementation note:** In the sample files, a `PID` segment appears anomalously
> mid-message (between two OBX rows near the end of the SpO2 waveform section). This
> appears to be an artifact of the upstream data pipeline that captured the samples,
> not a spec-defined behavior. Parsers should handle or ignore misplaced PID segments
> gracefully.

---

## ACK Message (PCD-02)

The aggregator expects an HL7 ACK in response to every outbound message before
sending the next. A minimal ACK contains:

```
MSH|^~\&|<receiver>|<facility>|||<timestamp>||ACK||P|2.3|||||||||
MSA|AA|<original_message_control_id>||||
```

MSA-1 of `AA` indicates acceptance. Failure to send an ACK will stall the feed.
The ACK version may be 2.3 even when the originating message is 2.6.

---

## DEC Feed Message Structure (PCD-01, Numeric Only)

The DEC feed uses a single OBR group per message with no waveform OBRs.

**OBR-4 service code:** `182777000^monitoring of patient^SCT`

The OBX structure follows the standard device hierarchy (MDS → VMD → Channel →
Metric) described in the OBX Sub-ID section below. All OBX value types are `NM`.

---

## WCM/WFM Feed Message Structure (PCD-01 with Waveforms)

A WCM message contains **two categories of OBR groups**, distinguished by the
service identifier in OBR-4:

### OBR|1 — Numeric Vitals Section

```
OBR|1|...|...|182777000^monitoring of patient^SCT|...
```

- Service code `182777000^monitoring of patient^SCT` (SNOMED CT)
- Contains NM-type OBX rows for all numeric vital signs
- One OBR-1 section per message, always first

### OBR|2..N — Waveform Sections (one per active channel)

```
OBR|N|...|...|69121^MDC_OBS_WAVE_CTS^MDC|...|<start_ts>|<stop_ts>|...
```

- Service code `69121^MDC_OBS_WAVE_CTS^MDC` (continuous waveform)
- OBR-7 = waveform window start timestamp
- OBR-8 = waveform window end timestamp (typically 6 seconds after start)
- One OBR per active waveform channel (ECG leads, ABP, PAP, CVP, SpO2 pleth, CO2)

**OBR-2 / OBR-3 (Placer/Filler order numbers):**
Both fields carry the same composite value:
`<device_EUI><timestamp>^<feed_name>^<device_EUI>^EUI-64`

---

## ACM Feed Message Structure (PCD-04, Alarms)

The ACM feed uses `ORU^R40^ORU_R40` messages sent asynchronously when an alarm is
detected. The aggregator is the Alert Reporter actor; the downstream system
is the Alert Manager.

### OBR|1 — Alarm Section

```
OBR|1|<uuid>^^urn:bernoulli^URN|<uuid>^^urn:bernoulli^URN|196616^MDC_EVT_ALARM^MDC|||<timestamp>||...
```

- Service code `196616^MDC_EVT_ALARM^MDC`
- OBR-7 = alarm event timestamp
- OBR-2 / OBR-3 carry a UUID rather than an EUI-64 device identifier

The OBX rows within the alarm OBR describe the alarm event using the device
hierarchy, then a set of alarm attribute rows:

```
OBX|1 | (blank)  | MDC_DEV_MON_PHYSIO_MULTI_PARAM_MDS | 1.0.0.0   | (MDS hierarchy marker)
OBX|2 | (blank)  | MDC_DEV_<VMD_TYPE>_VMD              | 1.<V>.0.0 | (VMD hierarchy marker)
OBX|3 | (blank)  | MDC_DEV_<CHAN_TYPE>_CHAN             | 1.<V>.<C>.0 | (Channel marker, if applicable)
OBX|4 | ST       | <alarm_event_code>                  | 1.<V>.<C>.1.1 | <alarm_label>   (alarm identity)
OBX|5 | NM       | <measured_parameter>                | 1.<V>.<C>.1.2 | <value>         (triggering value, if applicable)
OBX|6 | ST       | 68481^MDC_ATTR_EVENT_PHASE^MDC      | 1.<V>.<C>.1.3 | start|end|...
OBX|7 | ST       | 68482^MDC_ATTR_ALARM_STATE^MDC      | 1.<V>.<C>.1.4 | active|inactive|...
OBX|8 | ST       | 68483^MDC_ATTR_ALARM_INACTIVATION_STATE^MDC | 1.<V>.<C>.1.5 | enabled|disabled|...
OBX|9 | ST       | 68484^MDC_ATTR_ALARM_PRIORITY^MDC   | 1.<V>.<C>.1.6 | PN|HI|...
OBX|10| ST       | 68485^MDC_ATTR_ALERT_TYPE^MDC       | 1.<V>.<C>.1.7 | SP|...
```

**Alarm attribute OBX codes:**

| MDC Code | MDC Name | OBX-5 Values |
|---|---|---|
| 68480 | MDC_ATTR_ALERT_SOURCE | VMD code of the alerting source |
| 68481 | MDC_ATTR_EVENT_PHASE | `start`, `end` |
| 68482 | MDC_ATTR_ALARM_STATE | `active`, `inactive` |
| 68483 | MDC_ATTR_ALARM_INACTIVATION_STATE | `enabled`, `disabled` |
| 68484 | MDC_ATTR_ALARM_PRIORITY | `PN` (patient notice), `HI` (high), `LO` (low), `SP` (special) |
| 68485 | MDC_ATTR_ALERT_TYPE | `SP` (specific), other values |
| 196616 | MDC_EVT_ALARM | OBR service code — alarm OBR identifier |

### OBR|2 — Non-Continuous Waveform Snippet (optional)

When `Enable Waveform Export` is true and the source device provides waveform data,
an alarm message may include a second OBR with a waveform snippet:

```
OBR|2|...|...|69122^MDC_OBS_WAVE_NONCTS^MDC|||<start_ts>|<stop_ts>|...
```

- Service code `69122^MDC_OBS_WAVE_NONCTS^MDC` (non-continuous waveform)
- This distinguishes ACM waveform snippets from WCM continuous waveforms
  (`69121^MDC_OBS_WAVE_CTS^MDC`)
- The waveform OBX structure within this OBR is identical to the WCM waveform
  OBX pattern (NA data row, sample rate, data range, sentinels, scale factor)

### Alarm Status Notifications (PDC-05, inbound, optional)

The Alert Manager may optionally send PDC-05 `Report Alarm Status` messages back
to the aggregator. These provide context such as accepted or rejected. They are
logged by the Messaging Gateway service and appear in the Log of Pager Message
report. No parsing is required by the receiving integration unless it also acts
as an Alert Manager.

---

## OBX Sub-ID Hierarchy

All OBX rows use Sub-ID (OBX-4) to encode position within the device hierarchy.
The Sub-ID format is a dot-delimited path with four positions:

```
<MDS>.<VMD>.<Channel>.<Metric>
```

| Position | Meaning | Example value |
|---|---|---|
| 1 | MDS (Medical Device System) — always `1` | `1` |
| 2 | VMD ID (Virtual Medical Device) | `5`, `11`, `13`, `22`, `26` |
| 3 | Channel within VMD | `0` = VMD-level, `1`, `2`, `4` = specific channel |
| 4 | Metric within channel | `0` = channel-level, `1`+ = specific metric |

**Hierarchy marker OBX rows** (type blank, no value) announce a new VMD or Channel
context. They have Sub-ID ending in `.0.0` (VMD level) or `.<channel>.0` (channel
level) and OBX-11 = `X`.

**Example path for ECG heart rate:**
```
OBX|3|NM|147842^MDC_ECG_HEART_RATE^MDC|1.5.0.1|96|...
```
→ MDS=1, VMD=5 (ECG VMD), Channel=0 (VMD-level), Metric=1

In waveform OBR sections the Sub-ID gains additional depth for channel metadata:

```
1.5.0.1        = waveform samples for ECG Lead I on VMD 5
1.5.0.1.1      = MDC_ATTR_SAMPLE_RATE for that channel
1.5.0.1.2      = MDC_ATTR_DATA_RANGE for that channel
1.5.0.1.2.1    = first MDC_EVT_DATA_INVALID sentinel value
1.5.0.1.2.N    = Nth sentinel value
1.5.0.1.3      = MDC_ATTR_SA_MSMT_RES (measurement resolution / scale factor)
```

In ACM OBR sections the Sub-ID gains a fifth position for alarm attributes:

```
1.<V>.<C>.1.1  = alarm event identity (ST, the alarm code/label)
1.<V>.<C>.1.2  = triggering measured value (NM, if applicable)
1.<V>.<C>.1.3  = MDC_ATTR_EVENT_PHASE
1.<V>.<C>.1.4  = MDC_ATTR_ALARM_STATE
1.<V>.<C>.1.5  = MDC_ATTR_ALARM_INACTIVATION_STATE
1.<V>.<C>.1.6  = MDC_ATTR_ALARM_PRIORITY
1.<V>.<C>.1.7  = MDC_ATTR_ALERT_TYPE
```

---

## OBX Field Reference

| Field | Content |
|---|---|
| OBX-1 | Set ID (sequential integer within message) |
| OBX-2 | Value type: `NM` (numeric), `NA` (numeric array), `NR` (numeric range), `ST` (string, used in ACM), blank (hierarchy marker) |
| OBX-3 | Observation identifier: `<MDC_code>^<MDC_name>^MDC` |
| OBX-4 | Sub-ID (hierarchy path, see above) |
| OBX-5 | Value (or `^`-delimited sample array for NA type; alarm label string for ST type) |
| OBX-6 | Units: `<code>^<code>^UCUM` |
| OBX-7 | Reference range (used in ACM for alarm threshold, e.g. `14.7-25.5`) |
| OBX-8 | Abnormal flags (e.g. `H`, `L`; alarm priority flags in ACM: `H~PN~SP`) |
| OBX-11 | Observation result status: `R` (result final), `X` (not available/no result) |
| OBX-14 | Observation datetime (for NM vitals rows) |
| OBX-18 | Equipment instance identifier: `<instance>~<port>^<feed>^<EUI>^EUI-64` |
| OBX-20 | Observation site / anatomical label (e.g. `artery`, `pulmonary-artery`, `central-venous`, `unknown`) |

---

## Waveform OBX Structure (per channel, WCM and ACM waveform snippets)

Each waveform OBR section contains this pattern of OBX rows:

```
OBX|N  | (blank)  | MDC_DEV_MON_PHYSIO_MULTI_PARAM_MDS | 1.0.0.0     | (hierarchy: MDS)
OBX|N+1| (blank)  | MDC_DEV_<VMD_TYPE>_VMD             | 1.<V>.0.0   | (hierarchy: VMD)
OBX|N+2| NA       | MDC_<WAVE_PARAM>                   | 1.<V>.0.1   | <samples>   (waveform data)
OBX|N+3| NM       | MDC_ATTR_SAMPLE_RATE               | 1.<V>.0.1.1 | <rate>      (Hz)
OBX|N+4| NR       | MDC_ATTR_DATA_RANGE                | 1.<V>.0.1.2 | <min>^<max> (raw int16 range)
OBX|N+5| NM       | MDC_EVT_DATA_INVALID               | 1.<V>.0.1.2.1 | <sentinel>  (invalid value 1)
  ...  | NM       | MDC_EVT_DATA_INVALID               | 1.<V>.0.1.2.N | <sentinel>  (invalid value N)
OBX|M  | NM       | MDC_EVT_DATA_MISSING               | 1.<V>.0.1.2.N+1 | <sentinel> (missing value 1)
  ...  | NM       | MDC_EVT_DATA_MISSING               | 1.<V>.0.1.2.N+M | <sentinel> (missing value M)
OBX|K  | NM       | MDC_ATTR_SA_MSMT_RES               | 1.<V>.0.1.3 | <scale>     (units per LSB)
```

**Waveform sample encoding:** NA-type OBX-5 values are plain `^`-delimited signed
integers. No run-length encoding was observed in samples; all use
uncompressed sequential values (e.g. `-10^32^13^99^-10^35…`).

**Sample rate:** Reported as `264608^MDC_DIM_PER_SEC` per spec. Observed samples
use `s^s^UCUM` instead — parsers should handle both forms. See spec-parameters.md
for the open question on this discrepancy.

**Scale factor:** `MDC_ATTR_SA_MSMT_RES` gives the physical units per raw integer
LSB. Multiply raw sample values by this factor to get physical units. Per the spec,
if the scale factor is absent in the feed (possible for IP and CO2 waveforms),
refer to the device documentation.

---

## VMD IDs Observed in Samples

| VMD ID | MDC Code | Description |
|---|---|---|
| 5 | `69798^MDC_DEV_ECG_VMD` | ECG — leads and ST segments |
| 6 | `70666^MDC_DEV_ECG_RESP_VMD` | Respiration (thoracic impedance) |
| 11 | `69766^MDC_DEV_GEN_CONC_AWAY_VMD` | Airway gas / CO2 |
| 13 | `69854^MDC_DEV_METER_PRESS_BLD_VMD` | Invasive blood pressure |
| 16 | `70686^MDC_DEV_PRESS_BLD_NONINV_VMD` | Non-invasive blood pressure (NIBP) |
| 22 | `69642^MDC_DEV_ANALY_SAT_O2_VMD` | SpO2 / pulse oximetry |
| 26 | `69902^MDC_DEV_METER_TEMP_VMD` | Temperature |

The MDS (top-level device) always appears as:
`69965^MDC_DEV_MON_PHYSIO_MULTI_PARAM_MDS` with Sub-ID `1.0.0.0`.
