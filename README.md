# physiomon

Pipeline for ingesting, routing, and storing patient physiological data from
HL7 v2 / MLLP physiological monitor aggregators implementing IHE PCD profiles
(PCD-01, PCD-04).

## Architecture

```
Monitor aggregator  ─or─  physiosim/ (dev/test/demo)
         └──────── HL7 v2 over MLLP (TCP) ────────┘
                              │
                          mllp2nats      Go — receives MLLP, publishes raw bytes to NATS JetStream
                              │  NATS JetStream  stream: HL7   subject: hl7.inbound
                              ▼
                          service.py     Python — subscribes, parses HL7, writes to DuckDB staging
```

Three feed types, all delivered to a single configurable MLLP port (default 2575):

| Feed | HL7 type | Content |
|------|----------|---------|
| DEC  | ORU^R01  | Numeric vitals — HR, IBP, SpO₂, temperature, ST segments |
| ACM  | ORU^R40  | Alarm lifecycle events |
| WCM  | ORU^R01  | Numeric vitals + 12-lead ECG waveforms at 240 Hz |

## Layout

```
physiosim/    Mock HL7 generator and MLLP sender (dev/test/demo only)
  sender.py     send HL7 via MLLP — case files or stream mode
  generator.py  mock HL7 generators for all three feed types
  examples/     reference HL7 sample files (case*.hl7)
mllp2nats/    Go MLLP→NATS bridge (Windows service in production)
physiomon/    Python HL7 consumer
  service.py    long-running NATS subscriber → DuckDB staging (production)
  parser.py     parse_vectorized: HL7 bytes → Polars DataFrame
  config.py     all endpoints; overridable via PHYSIOMON_* env vars
test.py       unit + integration tests (run via physiomon's venv)
specs/        message schema, transport config, parameter reference
```

## Prerequisites

- **NATS server** with JetStream — macOS: `brew install nats-server`,
  Windows: `winget install NATS.io.nats-server`
- **Go 1.22+** to build `mllp2nats`
- **Python 3.14+** with [uv](https://docs.astral.sh/uv/)

## Quick start (demo)

Build `mllp2nats` once, then use `demo.py` to start everything:

```bash
cd mllp2nats && go build -o mllp2nats . && cd ..
python demo.py                            # fixed mode, 1 patient, 20 msgs
python demo.py --mode stream --patients 2
python demo.py --help                     # full sender.py option list
```

`demo.py` starts `nats-server`, `mllp2nats`, and `service.py` if they aren't
already running, and stops what it started on exit.

Staging output lands in `physiomon/out/staging/staging.duckdb`.

## Running manually

```bash
# Terminal 1 — infrastructure
nats-server -js
# Terminal 2
cd mllp2nats && ./mllp2nats
# Terminal 3 — production consumer
cd physiomon && python service.py
# Terminal 4 — send data
cd physiosim && python sender.py                            # case files
cd physiosim && python sender.py --mode fixed --count 20
```

## Production

`mllp2nats` runs as a Windows service (see [`mllp2nats/README.md`](mllp2nats/README.md)).
`service.py` runs as a long-running process. Endpoints are configured via
environment variables; see `physiomon/config.py`.

## Tests

```bash
cd physiomon
uv run --extra test python -m pytest ../test.py -v
```

Unit tests run unconditionally. Integration tests require NATS and `mllp2nats`
and skip gracefully if absent.

See [`docs/decisions.md`](docs/decisions.md) for design rationale and
[`specs/README.md`](specs/README.md) for message schema and field reference.
