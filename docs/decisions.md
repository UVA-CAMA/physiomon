# Design Decisions

## Storage format: parquet via DuckDB staging

Output is three parquet files (`numerics`, `waveforms`, `alarms`) compacted
from a local DuckDB staging database. DuckDB is the staging buffer because it
supports native `list[i16]` (needed for waveform values), type-preserving
parquet export, and zero-copy Polars→DuckDB writes via Arrow. Each message is
written to DuckDB and ACKed to NATS immediately — no in-memory buffer, no
flush-interval complexity.

---

## Write strategy: DuckDB staging → daily parquet archive

`service.py` appends every message to one DuckDB file per UTC day (see
*Staging layout* below). A compaction step (not yet implemented; format still
undecided) turns closed days into
per-location-per-day files:

```
out/
  staging/
    YYYYMMDD.duckdb         ← one per UTC day; today and yesterday held open
  archive/
    vitals/{year}/{unit}/{bed}/{date}.parquet
    waveforms/{year}/{unit}/{bed}/{date}.parquet
    alarms/{year}/{unit}/{bed}/{date}.parquet
```

Parquet files are written atomically (`.tmp` → rename).

---

## Staging layout: day files, one waveforms table, by-bed partitioning deferred

Decided 2026-11-11.

- **One DuckDB file per UTC day**, routed by MSH-7, falling back to NATS arrival
  time. Closed days can be moved or read without stopping the service.
- **Message key `msg_seq`** (NATS stream sequence). Writes are idempotent on
  redelivery. On a one-bed read, the `msg_seq` join was ~3x faster than the GUID.
- **Waveforms as packed int16 BLOBs in one `waveforms` table** for all beds.
- **Partitioning by bed is deferred to the archive step.** In a 700-bed local
  benchmark, per-bed tables (table per bed, per bed + signal, or column-per-signal)
  were:
  - 7–13x slower to ingest;
  - up to 2x larger;
  - prone to stalling single batches for 26–52 s at checkpoints.

  The one-table read of one bed-hour was 21 ms vs 0.5 ms for per-bed tables, which
  is fast enough. Revisit with real data when the archive format is chosen.
- **`checkpoint_threshold` 1GB** (DuckDB default 16MB checkpointed nearly every batch).

---

## Location-keyed archive; patient identity is optional enrichment

Archive files are keyed by `location (unit + bed) + date`, not by patient.
`PV1-3` (bed location) is always present in HL7 messages; `PID-3` (MRN)
arrives only when the device has an ADT match, which is not guaranteed.

A location-day file may contain rows for multiple patients (bed turnover
within a day is routine in an ICU). Patient-centric queries require either a
catalog lookup or a join through the `pid` table on `msg_ctrl_id`.

Patient identity lives in the `pid` staging table (one row per message that
carried a PID segment), joined to the other tables on `msg_ctrl_id`.
No placeholder rows (`UNKNOWN_xxx`) are created.

---

## Signal identity key: obs_code + channel

- `obs_code` = OBX-3.0 (MDC numeric code, e.g. `147842`)
- `channel` = OBX-4 sub_id `split(".")[2]` cast to `i32`

The sub_id format is `MDS.VMD.Channel.Metric`. Channel disambiguates multiple
instances of the same MDC code (e.g. two temperature probes: `MDC_TEMP`
channel 1 and channel 2). `obs_site` (OBX-20) is supplementary and unreliable
as a key.

---

## Long format, not wide

One row per OBX observation. Wide format would require column names like
`MDC_TEMP_1` / `MDC_TEMP_2` whose schema varies by patient and device
configuration. Long format handles variable channel counts with no schema
change.

---

## NATS durability: durable consumer + immediate ACK

`service.py` uses a named durable NATS consumer (`PHYSIOMON_CONSUMER_DURABLE`,
default `"physiomon-service"`). Each message is ACKed immediately after its
rows are committed to DuckDB. On restart, the durable consumer resumes from
the last ACKed position — no messages are lost or reprocessed.

`DeliverPolicy.ALL` is used on first consumer creation so messages published
before the service first starts (e.g. mllp2nats running ahead of physiomon on a
new deploy) are not skipped. The policy only applies when the durable is first
created; afterwards the consumer resumes from its last ACKed position. Deleting
or renaming the durable therefore replays the whole stream (up to 10 GB) — and
since staging writes are not yet idempotent, that produces duplicate rows.

---

## Vendor names kept out of open-source surface

Vendor and product names are kept out of
the public-facing surface. The interface is described generically as
IHE PCD-compliant (PCD-01, PCD-04). Vendor-specific details live in
`specs/priv/` which is gitignored.

---

## Storage estimates per patient-day

Measured on a full simulated day (14,400 DEC + 14,400 WCM + 4,320 ACM messages,
one patient, M-series Mac). Run `physiomon/benchmark.py` for throughput numbers.

**Waveforms dominate.** Numerics + alarms compress to ~1.7 MB/patient/day;
WCM accounts for ~99% of storage.

| Format | Size |
|--------|------|
| DuckDB staging (uncompressed) | 522 MB |
| Parsed parquet, zstd (archive) | 232 MB |
| Raw HL7 bytes, gzip | 261 MB |
| Raw HL7 bytes, xz | 244 MB |

Compressed raw HL7 is ~10% larger than parsed parquet. General-purpose
compressors exploit the ASCII repetition in waveform segments almost as
efficiently as columnar encoding, so archiving raw bytes is a viable strategy
if re-parseability matters more than query performance.

The DuckDB staging file is a transient buffer and should be truncated after
daily compaction to parquet — do not treat it as an archive.

At 600 beds with all feeds active: ~135 GB/day parquet. Without WCM: ~1 GB/day.

---

## Batch ingest: transaction cost dominates at small batch sizes

`service.py` collects up to `NATS_BATCH_SIZE` messages (default 500; was 50 until 2026-11-11) per
`fetch()` call and writes them in a single DuckDB transaction via
`ingest_batch`. Benchmark (`physiomon/benchmark.py`, 500 msgs, on-disk DuckDB,
M-series Mac):

| Feed | 1 at a time | batch=10 | batch=50 | batch=200 |
|------|-------------|----------|----------|-----------|
| DEC  |   77 msg/s  | 685 msg/s | 2,914 msg/s | 7,079 msg/s |
| ACM  |   72 msg/s  | 681 msg/s | 3,114 msg/s | 9,210 msg/s |
| WCM  |   52 msg/s  | 260 msg/s |   482 msg/s |   676 msg/s |

WCM is the bottleneck because each message carries ~274 OBX rows (12 ECG leads
× 1440 samples). DEC and ACM gains flatten out past batch=50; WCM keeps
improving because DuckDB write overhead is larger relative to parse time.

At batch=50 on-disk the service can sustain roughly 480 WCM msg/s. Real
monitoring rates are 1 msg/6 s per patient per feed, so 480 msg/s supports
~2,900 patients on WCM alone — well above any single-site deployment.

DuckDB is single-writer; scaling beyond one process requires one DB file per
process.

---

## Demo vs. production separation

`sender.py` — sends HL7 messages to mllp2nats via MLLP. Demo and test use
only. No NATS dependency; no parsing.

`service.py` — long-running NATS subscriber; writes to DuckDB. Production
artifact. Can run standalone or be orchestrated by `demo.py`.

`demo.py` (repo root) — starts NATS and mllp2nats if needed, starts
`service.py`, then runs `sender.py`. Developer convenience only.
