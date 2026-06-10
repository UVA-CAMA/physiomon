# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Architecture Overview

This repo is a pipeline for ingesting HL7 v2 messages from IHE PCD-compliant physiological monitor aggregators, plus a simulator for testing and development:

```
physiosim/          Python — mock HL7 generator and MLLP sender (dev/test/demo only)
    │  MLLP (TCP)
    ▼
mllp2nats/          Go — receives MLLP, publishes raw bytes to NATS JetStream
    │  NATS JetStream (stream: HL7, subject: hl7.inbound)
    ▼
physiomon/          Python — subscribes to NATS, parses HL7 → Polars DataFrames, writes DuckDB/parquet
```

Root-level files:
- `demo.py` — starts all services and runs physiosim/sender.py
- `test.py` — all tests (unit + integration); run via physiomon's venv
- `benchmark.py` — ingest throughput benchmarking

**Three feed types** (all currently on port 2575; spec-defined separate ports are DEC:2575, ACM:2576, WCM:2577):
- **DEC** — `ORU^R01` numeric vitals (HR, IBP, SpO2, temp, ST segments)
- **ACM** — `ORU^R40` alarm lifecycle events
- **WCM** — `ORU^R01` 12-lead ECG waveforms (1440 samples @ 240 Hz, 6-second batches)

**HL7 segment delimiter quirk:** Real aggregator messages use `\n` (LF), not `\r` (CR). `parse_vectorized` normalises both. Generators emit `\n`.

**Parser:** `parse_vectorized(messages: list[bytes])` — vectorised Polars, no Python loops over rows. One row per OBX segment with MSH/PID/PV1/OBR context forward-filled. Output split by `value_type`: NM/NR → numerics, NA → waveforms, ST → alarms.

**Deduplication** is keyed on `MSH-10` (message control ID, a UUID in live data). `dedup()` handles within-batch; `dedup_rolling(window)` handles cross-batch.

**Config** — all endpoints in `physiomon/config.py`, overridable via env vars. See `config.py` for the full list.

**NATS stream** is created automatically by `service.py` on first connect (compression=s2, max 10 GB).

**specs/** contains the authoritative upstream interface docs (see `specs/README.md` for index).
`specs/priv/` is gitignored and holds the original vendor PDF and unredacted sample files.

**docs/decisions.md** records key design choices. Read before making architectural changes.

## Commands

### mllp2nats (Go)

```bash
cd mllp2nats
go build -o mllp2nats .
./mllp2nats                      # defaults: port 2575, nats://127.0.0.1:4222, ACKs enabled
```

### physiomon (Python — run from `physiomon/`)

```bash
cd physiomon
python service.py                   # production consumer — writes to DuckDB staging
```

### physiosim (Python — run from `physiosim/`)

```bash
cd physiosim
python sender.py                                          # generate forever, 1 patient, 1s interval
python sender.py --files "examples/*.hl7"                # send example files once
python sender.py --count 20                              # stop after 20 msgs per patient per feed
python sender.py --patients 2 --duration 30.0            # 2 patients for 30 seconds
python sender.py --count 20 --interval 0.0               # as fast as possible
```

### Tests (run from `physiomon/`)

```bash
cd physiomon
uv run --extra test python -m pytest ../test.py -v      # all tests (unit always run; integration skips if NATS/mllp2nats absent)
```

### Demo (everything at once)

```bash
python demo.py                            # starts NATS + mllp2nats + service, sends fixed-mode messages
python demo.py --mode stream --patients 2
```

`demo.py` is at the repo root. Detects already-running services, skips starting them, and stops what it started on exit. Requires `mllp2nats` to be built first.

### Required services (if running manually)

```bash
nats-server -js &               # JetStream-enabled NATS
./mllp2nats/mllp2nats &         # MLLP bridge
python physiomon/service.py &   # HL7 consumer
```

## Key Design Notes

- **Parser must be data-element lossless.** Every segment field that carries a value in a real message must appear as a named column. A field being empty in test/sample data is not justification for omitting it from the schema. "No data loss" means any query answerable against the original HL7 message is answerable against the parsed output — byte-for-byte reconstruction is not required.

- **Stream creation ordering.** `mllp2nats` does not create the NATS stream — if the stream is missing, publishes fail silently. `service.py` creates it on first connect via `_ensure_stream`. Start `service.py` before sending data, or create the stream manually with the NATS CLI.

- **ACKs are disabled by default.** `mllp2nats` uses `--ack` to opt in. `physiosim/sender.py` drains the socket with a short timeout so it works in either mode without changes.

- **DuckDB is single-writer.** `staging.duckdb` is locked exclusively while `service.py` is running. You cannot open a second connection (e.g. for inspection) until the service is stopped. Use `read_only=True` only after the service has shut down.

- **Deduplication is not yet enforced at the infrastructure level.** Two improvements are planned but not implemented: (1) `mllp2nats` should set a `Nats-Msg-Id` header using MSH-10 to prevent publish-side duplicates in the stream; (2) the staging tables should have PRIMARY KEY / UNIQUE constraints with `INSERT OR IGNORE` to make consumer-side writes idempotent. Together these replace `dedup` / `dedup_rolling` in `parser.py`, which are currently dormant. Until then, the narrow crash window between DuckDB write and NATS ACK can produce duplicate rows.

- **`msg_id` is ephemeral.** `parse_vectorized` adds a `msg_id` UInt32 row-index column used internally for forward-fill grouping within a batch. `service.ingest` drops it before writing to DuckDB. Do not treat it as a stable identifier.

- **Waveform decode happens in `service.ingest`, not the parser.** `parse_vectorized` always returns `value` as a raw `VARCHAR` string. The `^`-delimited integer decode (`value` → `values: list[i16]`) is applied in `service.ingest` for NA-type rows only. `staging_waveforms` has `values SMALLINT[]`; `staging_numerics` and `staging_alarms` have `value VARCHAR`.

- **`pid_patient_id` is null in all test/case-file data.** The sample HL7 files have PID segments stripped. The column is always nullable; its presence depends on ADT matching in production.


---

## Behavioral Guidelines

*Adapted from [multica-ai/andrej-karpathy-skills](https://github.com/multica-ai/andrej-karpathy-skills/blob/main/CLAUDE.md). These bias toward caution over speed — use judgment on trivial tasks.*

### 1. Think Before Coding

Before implementing: state assumptions explicitly, surface tradeoffs, present multiple interpretations rather than picking silently, and ask when something is unclear. Don't hide confusion.

### 2. Simplicity First

Minimum code that solves the problem. No speculative features, no abstractions for single-use code, no unrequested configurability, no error handling for impossible scenarios. If it could be 50 lines, don't write 200.

### 3. Surgical Changes

Touch only what the request requires. Don't improve adjacent code, reformat, or refactor things that aren't broken. Match existing style. If your changes create unused imports/variables/functions, remove them — but leave pre-existing dead code alone and mention it instead.

### 4. Goal-Driven Execution

Transform tasks into verifiable goals before starting. For multi-step work, state a brief plan with a verify step for each item. Strong success criteria allow independent looping; weak ones ("make it work") require constant clarification.

### 5. Challenge Vague Requirements

Don't accept "more normalized," "doesn't discard anything," or "something like X" as design input. Push back and ask for a concrete schema, a concrete constraint, or a concrete example before writing code toward it. A half-hour conversation now is better than a rewrite later. Apply this even when the user seems confident — especially then.
