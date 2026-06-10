# Cleanup Plan

## Context

The `physiomon` Python package grew incrementally: first a demo batch runner, then
a production service, with each iteration adding code alongside rather than
replacing what was there. The result is four NATS consumer implementations,
three consumer implementations in one file (`cli.py`), MLLP framing in two
places, two test files testing overlapping things, and three planning docs
whose substance has been implemented and superseded.

This document proposes a cleanup that reduces the codebase to what is actually
needed and consolidates documentation to what is actually useful.

---

## Code changes

### What stays, what changes, what goes

| File | After cleanup |
|------|--------------|
| `service.py` | Unchanged — production consumer |
| `parser.py` | Unchanged |
| `config.py` | Unchanged |
| `generator.py` | Unchanged |
| `sender.py` (new) | Replaces the send half of `cli.py`: MLLP envelope, case-file loader, mock send modes |
| `cli.py` | Deleted |
| `demo.py` (root) | Updated to use `sender.py` instead of `cli.py` |

### What `sender.py` contains

The send-side logic extracted from `cli.py` — no NATS consumer, no
`write_results`, no `parse_vectorized`. Three send modes driven by a minimal
argparse, equivalent to the current `files`, `fixed`, and `stream` modes:

```
python sender.py                                    # send case files once
python sender.py --mode fixed --count 20 --patients 1
python sender.py --mode stream --patients 2 --duration 10
```

The MLLP envelope (`\x0b` + msg + `\x1c\x0d`) appears once, in `sender.py`.
`generator.py`'s `run_feed` currently has it inline — that becomes a call to
the shared function.

### NATS consumer count: 4 → 1

Currently:
1. `cli.py` files mode
2. `cli.py` `run_fixed`
3. `cli.py` `run_stream`
4. `service.py` `run`

After: `service.py` only. Demo and test infrastructure sends messages and
inspects DuckDB directly — no second consumer needed.

---

## Test changes

### Result

A single `tests/test.py` replacing both existing test files. Requires NATS +
mllp2nats (skips gracefully if absent). Covers:
- `open_staging_db` and `ingest` unit behaviour (no NATS needed, runs always)
- End-to-end: starts `service.run()` as a background asyncio task, sends case
  files via MLLP using `sender`, asserts row counts and column types in DuckDB
- Restart durability: stop service, restart, verify no double-ingestion

`test_pipeline.py` and `test_service.py` are deleted.

---

## Documentation changes

### Keep (trim in place)

**`docs/decisions.md`** — remove the HDF5/HSDI discussion (the prior system
is replaced, not relevant to getting this system running), the pending SQL
Server ALTER statements (catalog design is TBD), and the single-port decision
(obvious, doesn't need documentation). What remains: write strategy, storage
format rationale, location-keyed file design, signal identity key, vendor
naming policy.

**`specs/`** — keep as-is. Authoritative HL7/IHE interface docs.

**`CLAUDE.md`** — update to reflect the new file layout (`sender.py` replaces
`cli.py` for demo use, `service.py` is the production entry point).

### Archive (move to `docs/history/`)

| File | Why archived |
|------|-------------|
| `docs/parser_redesign.md` | Implemented. The schema is in the code. |
| `docs/staging_writes.md` | Implemented. The service is in the code. |
| `docs/cleanup_plan.md` | This file — once executed, it's history. |

`docs/history/` is not linked from active docs. It exists so prior design
reasoning is not lost if questions arise later.

### Create

Update `README.md` (root) with setup content:
- Prerequisites (NATS server with JetStream, mllp2nats binary)
- How to run the production service (`python service.py`)
- How to send mock data (`python sender.py`)
- How to run tests
- Where outputs land (`out/staging/staging.duckdb`)

---

## Sequencing

Order matters to keep the test suite green throughout:

1. Write `sender.py` and verify `demo.py` works with it.
2. Write `tests/test.py` against `service.py` + `sender.py`.
3. Delete `cli.py`, `test_pipeline.py`, and `test_service.py` once `tests/test.py` passes.
4. Remove the inline MLLP envelope from `generator.py`.
5. Trim `docs/decisions.md`, create `docs/history/`, move archived docs.
6. Update `CLAUDE.md` and `README.md`.

