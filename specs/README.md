# specs/

Reference documentation for the upstream HL7 interface. These describe what the
aggregator *sends* — they are not documentation of this project's own design.

| File | Contents |
|------|----------|
| [spec-message-schema.md](spec-message-schema.md) | Segment structure for all three feeds (DEC, ACM, WCM): MSH, PV1, OBR, OBX field mappings and the device hierarchy sub-ID scheme |
| [spec-transport-config.md](spec-transport-config.md) | MLLP transport, ACK behaviour, timing, backlog buffering, device identification |
| [spec-parameters.md](spec-parameters.md) | MDC parameter codes and names for numeric OBX observations |
| [manual.md](manual.md) | Narrative notes derived from the vendor interface manual |
| [8.1_waveforms.csv](8.1_waveforms.csv) | Waveform channel definitions |
| [8.2_measurements.csv](8.2_measurements.csv) | Numeric measurement definitions |
| [8.3_gw_measurements.csv](8.3_gw_measurements.csv) | Gateway-level measurement definitions |
| [8.4_alarms.csv](8.4_alarms.csv) | Alarm code definitions |

`priv/` contains the original vendor PDF and unredacted sample files; it is gitignored.
