# IHE PCD Aggregator MDC Parameter Reference

MDC codes follow the ISO/IEEE 11073-10101 "Rosetta" nomenclature standard. This
document lists the codes observed in WFM/WCM feed samples and ACM alarm messages,
along with their meanings. It is not exhaustive — the full 11073-10101 standard is
the authoritative reference for any code not listed here.

For the complete per-parameter lookup tables (device hierarchy path, data type, and
unit of measure for every supported metric), see the companion CSV files:

- `params-waveforms.csv` — Section 8.1: waveform channels
- `params-bridge-measurements.csv` — Section 8.2: bridge numeric measurements
- `params-gateway-measurements.csv` — Section 8.3: gateway numeric measurements
- `params-alarms.csv` — Section 8.4: bridge alarm events

> **Note on spec unit-of-measure errors:** The PDF source tables in sections 8.2
> and 8.3 contain numerous apparent copy-paste errors in the Unit of Measure column
> — for example, SpO2 pulse rate listed as `mm[Hg]^mm[Hg]^UCUM`, temperature
> parameters listed as `Cel^Cel^UCUM` when the preceding rows used `mm[Hg]`, and
> respiration rate listed as `Cel^Cel^UCUM`. These have been flagged in the CSV
> `spec_uom_note` column. The correct units are listed in this document and should
> be treated as authoritative over the CSV `unit_of_measure` column for those rows.

---

## Device Hierarchy Codes (OBX-3, hierarchy marker rows)

| MDC Code | MDC Name | Description |
|---|---|---|
| 69965 | MDC_DEV_MON_PHYSIO_MULTI_PARAM_MDS | Multi-parameter physiological monitor (MDS / top-level device) |
| 69798 | MDC_DEV_ECG_VMD | ECG virtual medical device |
| 70666 | MDC_DEV_ECG_RESP_VMD | ECG/respiration VMD |
| 69766 | MDC_DEV_GEN_CONC_AWAY_VMD | Airway gas concentration VMD |
| 69854 | MDC_DEV_METER_PRESS_BLD_VMD | Invasive blood pressure VMD |
| 69855 | MDC_DEV_METER_PRESS_BLD_CHAN | Invasive blood pressure channel |
| 70686 | MDC_DEV_PRESS_BLD_NONINV_VMD | Non-invasive blood pressure VMD |
| 70687 | MDC_DEV_PRESS_BLD_NONINV_CHAN | Non-invasive blood pressure channel |
| 69642 | MDC_DEV_ANALY_SAT_O2_VMD | SpO2 / pulse oximetry VMD |
| 69643 | MDC_DEV_ANALY_SAT_O2_CHAN | SpO2 channel |
| 69902 | MDC_DEV_METER_TEMP_VMD | Temperature VMD |
| 69903 | MDC_DEV_METER_TEMP_CHAN | Temperature channel |

Additional VMD types appearing in Gateway measurements (section 8.3) but not
in Bridge waveform or alarm tables:

| MDC Code | MDC Name | Description |
|---|---|---|
| 69746 | MDC_DEV_CALC_HEMO_VMD | Hemodynamic calculations VMD |
| 69670 | MDC_DEV_ANALY_CARD_OUTPUT_VMD | Cardiac output VMD |
| 69671 | MDC_DEV_ANALY_CARD_OUTPUT_CHAN | Cardiac output channel |
| 69806 | MDC_DEV_METER_CONC_URINE_VMD | Urine concentration VMD |
| 70730 | MDC_DEV_O2_VEN_SAT_VMD | Venous O2 saturation VMD |
| 0 | MDC_DEV_CALC_CARD_VMD | Calculated cardiac VMD (VMD ID 2) |
| 0 | MDC_DEV_CALC_PULM_VMD | Calculated pulmonary VMD (VMD ID 19) |
| 0 | MDC_DEV_ANALY_BLD_CHEM_MULTI_PARAM_VMD | Blood chemistry multi-parameter VMD (VMD ID 18) |
| 0 | MDC_DEV_ANALY_CARD_OUTPUT_CTS_VMD | Continuous cardiac output VMD (VMD ID 4) |

---

## Numeric Vital Sign Codes (OBX-2 = NM)

### ECG / Cardiac

| MDC Code | MDC Name | Typical Units |
|---|---|---|
| 147842 | MDC_ECG_HEART_RATE | {beat}/min |
| 148066 | MDC_ECG_V_P_C_RATE | {beat}/min (ventricular premature complex rate) |
| 131841 | MDC_ECG_AMPL_ST_I | uV (ST amplitude, Lead I) |
| 131842 | MDC_ECG_AMPL_ST_II | uV (ST amplitude, Lead II) |
| 131901 | MDC_ECG_AMPL_ST_III | uV |
| 131843 | MDC_ECG_AMPL_ST_V1 | uV |
| 131844 | MDC_ECG_AMPL_ST_V2 | uV |
| 131845 | MDC_ECG_AMPL_ST_V3 | uV |
| 131846 | MDC_ECG_AMPL_ST_V4 | uV |
| 131847 | MDC_ECG_AMPL_ST_V5 | uV |
| 131848 | MDC_ECG_AMPL_ST_V6 | uV |
| 131902 | MDC_ECG_AMPL_ST_AVR | uV |
| 131903 | MDC_ECG_AMPL_ST_AVL | uV |
| 131904 | MDC_ECG_AMPL_ST_AVF | uV |

### Respiration

| MDC Code | MDC Name | Typical Units |
|---|---|---|
| 151578 | MDC_TTHOR_RESP_RATE | {breath}/min |

### Airway Gas / CO2

| MDC Code | MDC Name | Typical Units |
|---|---|---|
| 151712 | MDC_CONC_AWAY_CO2_EXP | mm[Hg] (end-tidal CO2) |
| 151716 | MDC_CONC_AWAY_CO2_INSP | mm[Hg] (inspired CO2) |

### Invasive Blood Pressure

| MDC Code | MDC Name | Typical Units | Notes |
|---|---|---|---|
| 150033 | MDC_PRESS_BLD_ART_SYS | mm[Hg] | Arterial systolic |
| 150034 | MDC_PRESS_BLD_ART_DIA | mm[Hg] | Arterial diastolic |
| 150035 | MDC_PRESS_BLD_ART_MEAN | mm[Hg] | Arterial mean |
| 149522 | MDC_BLD_PULS_RATE_INV | {beat}/min | Invasive pulse rate |
| 150018 | MDC_PRESS_BLD_DIA | mm[Hg] | Generic diastolic (used for PAP) |
| 150019 | MDC_PRESS_BLD_MEAN | mm[Hg] | Generic mean (used for PAP) |
| 150045 | MDC_PRESS_BLD_ART_PULM_SYS | mm[Hg] | Pulmonary artery systolic |
| 150085 | MDC_PRESS_BLD_VEN_CENT_SYS | mm[Hg] | Central venous systolic |
| 150086 | MDC_PRESS_BLD_VEN_CENT_DIA | mm[Hg] | Central venous diastolic |
| 150087 | MDC_PRESS_BLD_VEN_CENT_MEAN | mm[Hg] | Central venous mean |

### Non-Invasive Blood Pressure

| MDC Code | MDC Name | Typical Units |
|---|---|---|
| 150300 | MDC_PRESS_CUFF | mm[Hg] |

### SpO2 / Pulse Oximetry

| MDC Code | MDC Name | Typical Units |
|---|---|---|
| 149530 | MDC_PULS_OXIM_PULS_RATE | {beat}/min |
| 150456 | MDC_PULS_OXIM_SAT_O2 | % |
| 160324 | MDC_SPO2_SIGNAL_QUALITY_INDEX | % |

### Temperature

| MDC Code | MDC Name | Typical Units |
|---|---|---|
| 150344 | MDC_TEMP | Cel |

---

## Waveform Channel Codes (OBX-2 = NA)

| MDC Code | MDC Name | Typical Sample Rate | Units |
|---|---|---|---|
| 131329 | MDC_ECG_ELEC_POTL_I | 240 Hz | uV |
| 131330 | MDC_ECG_ELEC_POTL_II | 240 Hz | uV |
| 131331 | MDC_ECG_ELEC_POTL_V1 | 240 Hz | uV |
| 131389 | MDC_ECG_ELEC_POTL_III | 240 Hz | uV |
| 131390 | MDC_ECG_ELEC_POTL_AVR | 240 Hz | uV |
| 131391 | MDC_ECG_ELEC_POTL_AVL | 240 Hz | uV |
| 131392 | MDC_ECG_ELEC_POTL_AVF | 240 Hz | uV |
| 151700 | MDC_CONC_AWAY_CO2 | 60 Hz | mm[Hg] |
| 150016 | MDC_PRESS_BLD | 120 Hz | mm[Hg] (invasive BP waveform, all channels) |
| 150452 | MDC_PULS_OXIM_PLETH | 60 Hz | % (SpO2 pleth waveform) |

---

## Waveform Channel Metadata Codes

These appear as NM or NR rows immediately following the NA waveform data row,
providing metadata needed to interpret the raw integer samples.

| MDC Code | MDC Name | Type | Description |
|---|---|---|---|
| 68320 | MDC_ATTR_SAMPLE_RATE | NM | Samples per second |
| 68323 | MDC_ATTR_DATA_RANGE | NR | Raw int16 range as `<min>^<max>` |
| 197376 | MDC_EVT_DATA_INVALID | NM | Sentinel value meaning "data invalid" |
| 197378 | MDC_EVT_DATA_MISSING | NM | Sentinel value meaning "data missing" |
| 67945 | MDC_ATTR_SA_MSMT_RES | NM | Scale factor: physical units per raw LSB |

Multiple `MDC_EVT_DATA_INVALID` and `MDC_EVT_DATA_MISSING` rows may appear per
channel, each with a different sentinel integer. Samples equal to any of these
values must be treated as invalid/missing rather than as real physiological data.

---

## ACM Alarm Attribute Codes (OBX-2 = ST)

These codes appear in ACM PCD-04 alarm messages as string-typed OBX rows following
the alarm event identity row. They describe the state and classification of the alarm.

| MDC Code | MDC Name | Description |
|---|---|---|
| 68480 | MDC_ATTR_ALERT_SOURCE | VMD code of the device generating the alarm |
| 68481 | MDC_ATTR_EVENT_PHASE | Phase of the alarm event: `start` or `end` |
| 68482 | MDC_ATTR_ALARM_STATE | Current alarm state: `active` or `inactive` |
| 68483 | MDC_ATTR_ALARM_INACTIVATION_STATE | Whether alarms are enabled: `enabled` or `disabled` |
| 68484 | MDC_ATTR_ALARM_PRIORITY | Priority level: `PN` (patient notice), `HI` (high), etc. |
| 68485 | MDC_ATTR_ALERT_TYPE | Alert classification: `SP` (specific), etc. |
| 196616 | MDC_EVT_ALARM | OBR service code identifying the OBR as an alarm report |

---

## ACM Alarm Event Codes

These appear in OBX-3 of the alarm identity row (Sub-ID position `.1.1`) within an
ACM OBR. The full list of supported codes per VMD and channel is in `params-alarms.csv`.
Abbreviated summary of alarm event code ranges by category:

### Physiological Limit Alarms

| MDC Code | MDC Name | Description |
|---|---|---|
| 196648 | MDC_EVT_HI | Parameter exceeded high limit |
| 196670 | MDC_EVT_LO | Parameter fell below low limit |

### Technical / Device Alarms

| MDC Code | MDC Name | Description |
|---|---|---|
| 196630 | MDC_EVT_DISCONN | Pressure channel disconnected |
| 196746 | MDC_EVT_CALIB_FAIL | Calibration failure |
| 196748 | MDC_EVT_COMM_LOST | Communication lost (ECG VMD) |
| 196838 | MDC_EVT_CUFF_NOT_DEFLATED | NIBP cuff failed to deflate |
| 196880 | MDC_EVT_LEAD_OFF | ECG lead off |
| 196882 | MDC_EVT_LEADS_OFF | ECG/respiration leads off |
| 196892 | MDC_EVT_MODULE_DISCONN | Pressure module disconnected |
| 196918 | MDC_EVT_SENSOR_MALF | SpO2 sensor malfunction |
| 196920 | MDC_EVT_SENSOR_PROB | SpO2 sensor problem |
| 196944 | MDC_EVT_XDUCR_DISCONN | SpO2 transducer disconnected |
| 197040 | MDC_EVT_WAVE_ARTIF_ERR | Waveform artifact error |
| 150392 | MDC_EVT_SENSOR_DISCONN | Temperature sensor disconnected |
| 202780 | MDC_EVT_STAT_OPT_MOD_SENSOR_CONN | Optional module sensor connected |
| 202798 | MDC_EVT_STAT_UNCALIB | Uncalibrated state |
| 202842 | MDC_EVT_STAT_WAVE_LEARN | Waveform learning (respiration VMD) |
| 203268 | MDC_EVT_ADVIS_CALIB_CHK | Advisory: calibration check required |

### NBP-Specific Technical Alarms

| MDC Code | MDC Name | Description |
|---|---|---|
| 0 | MDCX_EVT_CUFF_PRESS_OVER_200_MMHG | Cuff pressure exceeded 200 mmHg |
| 0 | MDCX_EVT_CUFF_PRESS_OVER_300_MMHG | Cuff pressure exceeded 300 mmHg |
| 0 | MDCX_EVT_HW_FAULT | Hardware fault |
| 0 | MDCX_EVT_NBP_TIMEOUT | NBP measurement timeout |
| 0 | MDCX_EVT_TIME_PD_NBP_MSMT_OVER_3_MIN | NBP measurement duration exceeded 3 minutes |

### ECG Arrhythmia Alarms

| MDC Code | MDC Name | Description |
|---|---|---|
| 0 | MDCX_EVT_ECG_IDIOV_RHY_ACCEL | Idioventricular rhythm accelerated |
| 0 | MDCX_EVT_ECG_V_BRADY | Ventricular bradycardia |
| 0 | MDCX_EVT_STAT_SIG_ARTIFACT | Signal artifact detected |
| 0 | MDCX_EVT_STAT_SIG_PROC_ARRHY_SUSPENDED | Arrhythmia processing suspended |
| 199684 | MDC_EVT_ECG_ASYSTOLE | Asystole |
| 199690 | MDC_EVT_ECG_BIGEM | Bigeminy |
| 199692 | MDC_EVT_ECG_SINUS_BRADY | Sinus bradycardia |
| 199716 | MDC_EVT_ECG_PAUSE | ECG pause |
| 199728 | MDC_EVT_ECG_TACHY | Tachycardia |
| 199734 | MDC_EVT_ECG_TRIGEM | Trigeminy |
| 199736 | MDC_EVT_ECG_ATR_FIB | Atrial fibrillation |
| 199806 | MDC_EVT_ECG_V_FIB | Ventricular fibrillation |
| 199812 | MDC_EVT_ECG_V_P_C | Ventricular premature complex |
| 199814 | MDC_EVT_ECG_V_P_C_RonT | VPC R-on-T |
| 199818 | MDC_EVT_ECG_V_P_C_PAIR | VPC pair |
| 199832 | MDC_EVT_ECG_V_TACHY | Ventricular tachycardia |
| 199834 | MDC_EVT_ECG_V_TACHY_NON_SUST | Non-sustained ventricular tachycardia |
| 199874 | MDC_EVT_ECG_ARRHY | Generic arrhythmia |

### SpO2 / Respiration Status Alarms

| MDC Code | MDC Name | Description |
|---|---|---|
| 0 | MDCX_EVT_PROBE_INCOMPAT | Incompatible SpO2 probe |
| 0 | MDCX_EVT_SIG_QUAL_LO | Low signal quality |
| 0 | MDCX_EVT_SPO2_PULSE_SEARCH | SpO2 pulse search in progress |
| 0 | MDCX_EVT_TTHOR_APNEA | Thoracic apnea (respiration VMD) |

> **Note on MDCX codes:** Codes shown as `0` are vendor-extended codes (`MDCX_` prefix)
> that use MDC code `0` in OBX-3, with the vendor name as the text component
> (e.g. `0^MDCX_EVT_TTHOR_APNEA^MDC`). They are not part of the ISO/IEEE 11073-10101
> Rosetta standard.

> **Note on generic alert code:** Code `0^MDCX_EVT_ALERT^MDC` appears as a
> top-level MDS-scoped alarm entry in the supported parameters table with no VMD,
> channel, or unit. Its exact usage is not further described in the spec.

---

## Observation Site Labels (OBX-20)

OBX-20 carries a plain-text anatomical site label for invasive pressure channels:

| Label | Meaning |
|---|---|
| `artery` | Systemic arterial line |
| `pulmonary-artery` | Pulmonary artery catheter (PAP) |
| `central-venous` | Central venous pressure (CVP) |
| `unknown` | Site not specified or not applicable |

SpO2 channels also carry `unknown` in OBX-20. ECG channels do not use OBX-20
except for respiration rate (`II` indicating Lead II impedance method).
