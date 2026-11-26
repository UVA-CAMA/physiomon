# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Architecture Overview

This repo is a pipeline for ingesting HL7 v2 messages from IHE PCD-compliant physiological monitor aggregators, plus a simulator for testing and development:

```
physiosim/          Python — mock HL7 generator and MLLP sender (dev/test/demo only)
    │  MLLP (TCP)
    ▼
mllp-bridge/        Go — receives MLLP, publishes raw bytes to NATS JetStream (mllp2nats)
    │  NATS JetStream (stream: HL7, subject: hl7.inbound)
    ▼
physiomon/          Python — subscribes to NATS, parses HL7 → Polars DataFrames, writes DuckDB/parquet
```

Root-level files:
- `demo.py` — starts all services and runs physiosim/sender.py
- `test.py` — all tests (unit + integration); run via physiomon's venv
- `benchmark.py` — ingest throughput benchmarking

**Three feed types**, one MLLP port each in production (DEC:2575, ACM:2576, WCM:2577 — one `mllp2nats` process listening on all three, `--listen DEC=:2575 --listen ACM=:2576 --listen WCM=:2577`, publishing to `hl7.inbound`; see `mllp-bridge/run-all.bat`). `physiosim` sends all three to 2575 by default:
- **DEC** — `ORU^R01` numeric vitals (HR, IBP, SpO2, temp, ST segments)
- **ACM** — `ORU^R40` alarm lifecycle events
- **WCM** — `ORU^R01` 12-lead ECG waveforms (1440 samples @ 240 Hz, 6-second batches)

**HL7 segment delimiter quirk:** Real aggregator messages use `\n` (LF), not `\r` (CR). `parse_vectorized` normalises both. Generators emit `\n`.

**Parser:** `parse_vectorized(messages: list[bytes], seqs) -> ParsedMessage` — vectorised Polars, no Python loops over rows. Returns per-segment DataFrames (`msh`, `pid`, `pv1`, `obr`, `obx`), all keyed on `msg_seq` (the NATS stream sequence); `msh` also keeps `msg_ctrl_id` (MSH-10). `service.ingest_batch` splits `obx` by `value_type`: NM/NR → numerics, NA → waveforms, ST → alarms.

**Staging** (DuckDB): one file per UTC day, `out/staging/YYYYMMDD.duckdb`, each with tables `msh`, `pid`, `pv1`, `obr`, `numerics`, `waveforms`, `signals`, `alarms` — join on `msg_seq`. A message's day is its MSH-7 timestamp (`msh.msg_ts`, UTC), or its NATS arrival time (`received_at`) when MSH-7 is missing or has no UTC offset. `service.Staging` attaches day files to one in-memory session: today and yesterday stay attached; older days are reopened for late data; a day whose file was moved away goes to `late/YYYYMMDD.duckdb`. `numerics` and `msh` carry `feed` (DEC/WCM/ACM). `signals` holds the per-channel waveform attributes (sample rate, data range, sentinels), not `numerics`.

**Idempotent writes:** `ingest_batch` skips messages whose `msg_seq` the day file already holds, so NATS redelivery doesn't duplicate rows. `dedup()` / `dedup_rolling()` in `parser.py` (MSH-10) are dormant.

**Config** — all endpoints in `physiomon/config.py`, overridable via env vars. See `config.py` for the full list.

**NATS stream** is created automatically by `service.py` on first connect (subjects=`hl7.inbound`, compression=s2, max 10 GB).

**specs/** contains the authoritative upstream interface docs (see `specs/README.md` for index).
`specs/priv/` is gitignored and holds the original vendor PDF and unredacted sample files.

**docs/decisions.md** records key design choices. Read before making architectural changes.

## Commands

### mllp-bridge (Go)

```bash
cd mllp-bridge
go build -o mllp2nats ./cmd/mllp2nats
./mllp2nats                      # defaults: one listener on :2575, nats://127.0.0.1:4222, ACKs disabled
go test ./...
```

### physiomon (Python — run from `physiomon/`)

```bash
cd physiomon
python service.py                   # production consumer — writes to DuckDB staging
python progress.py                  # processed vs received rate (speedup factor), pending, catch-up ETA
python export.py --out <dir>        # dump each day file's tables to zstd parquet, <dir>/<day>/ (stop service.py first)
python export_deid.py --out <dir> --keys <dir>  # de-identified export: pseudonyms, per-bed date shift to 2000-01-01; keys stay on the server
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
python demo.py                            # starts NATS + mllp2nats + service, sends simulated messages
python demo.py --patients 2 --duration 30
```

`demo.py` is at the repo root. Detects already-running services, skips starting them, and stops what it started on exit. Requires `mllp2nats` to be built first.

### Required services (if running manually)

```bash
nats-server -js &               # JetStream-enabled NATS
./mllp-bridge/mllp2nats &       # MLLP bridge
python physiomon/service.py &   # HL7 consumer
```

## Key Design Notes

- **Parser must be data-element lossless.** Every segment field that carries a value in a real message must appear as a named column. A field being empty in test/sample data is not justification for omitting it from the schema. "No data loss" means any query answerable against the original HL7 message is answerable against the parsed output — byte-for-byte reconstruction is not required.

- **Stream creation ordering.** `mllp2nats` does not create the NATS stream — if the stream is missing, every publish fails and mllp2nats closes the sender's connection (logged as a publish error). `service.py` creates it on first connect via `_ensure_stream`. Start `service.py` before sending data, or create the stream with the NATS CLI (command in `mllp-bridge/README.md`).

- **ACKs are disabled by default and real installations don't use them.** Per the vendor, aggregators in the field run with `Wait For ACK` off, even though the spec docs describe ACK as the default. `mllp2nats --ack` exists but is not expected to be needed. `physiosim/sender.py` drains the socket with a short timeout so it works in either mode without changes.

- **DuckDB is single-writer.** Attached day files (today, yesterday, and any reopened day) are locked exclusively while `service.py` is running; older day files can be read directly. To read the attached days live, set `PHYSIOMON_QUACK_LISTEN` + `PHYSIOMON_QUACK_TOKEN`: `Staging.serve` serves the session over DuckDB Quack (localhost), with `<table>_all` views over the attached days in the default catalog. Readers see committed batches only. `_replace_views` must run before a `DETACH`, or the views name a missing day.

- **Consumer-side writes are idempotent; publish-side duplicates are not yet prevented.** A redelivered message has the same `msg_seq` and is skipped. A message published twice (e.g. a sender retry) gets two sequences and is stored twice; `mllp2nats` should set a `Nats-Msg-Id` header from MSH-10 to stop that (planned).
- **One transaction per day file.** DuckDB can write only one attached database per transaction, so a batch spanning midnight commits once per day. The `msg_seq` skip makes a retry after a partial failure safe.

- **`msg_seq` must be unique within a batch.** It's the NATS stream sequence in the service; tests pass their own. It resets only if the stream is deleted and recreated.

- **Waveform encoding happens in `service.ingest_batch`, not the parser.** `parse_vectorized` always returns `value` as a raw `VARCHAR` string. For NA rows, `service._pack_int16` packs the `^`-delimited samples into `samples BLOB` (little-endian int16; decode with `np.frombuffer(b, "<i2")`). `waveforms.value` is NULL when that is lossless and keeps the original text when any element isn't an int16.

- **The `pid` table is empty for case-file data.** The sample HL7 files have PID segments stripped (the `physiosim` generator does emit PID). PID is present only when the aggregator has an ADT match, so a message may have no `pid` row.


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
