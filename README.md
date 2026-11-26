# physiomon

Tools for collecting physiological data from bedside monitors: vitals, waveforms and alarms. Two pipelines, one for each way the data leaves the monitoring network:

| Pipeline | Source | Folders |
|---|---|---|
| **HL7** | Monitor aggregators sending HL7 v2 over MLLP, following the IHE PCD profiles (PCD-01, PCD-04) | `mllp-bridge/`, `physiomon/`, `physiosim/`, `specs/` |
| **HSDI** | Gateways speaking HSDI, the High Speed Data Interface, over TCP | `hsdi-bridge/`, `hsdi-decode/` |

Both follow the same design: a small Go bridge receives what the source sends and publishes it unchanged to a [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream) stream, and separate programs decode and store it from there. The stream decouples receiving from processing: a decoder can be stopped, fixed and run again over what the bridge kept.

```
HL7:   aggregator ──MLLP──▶ mllp2nats ──▶ JetStream HL7 ──▶ service.py ──▶ DuckDB day files
HSDI:  gateway ──HSDI──▶ hsdi2nats ──▶ JetStream HSDI_RAW, HSDI_STATE ──▶ hsdi-decode ──▶ JetStream HSDI_DECODED
```

**Everything these programs receive is protected health information.** The raw streams, decoded output and day files carry patient identifiers and dates. Each part's README says what it carries.

## Contents

| Folder | Language | What it is |
|---|---|---|
| [`mllp-bridge/`](mllp-bridge/README.md) | Go | `mllp2nats`: receives HL7 over MLLP on one port per feed and publishes each message to JetStream. |
| [`physiomon/`](HL7.md) | Python | `service.py`: parses HL7 into tables and writes one DuckDB file per day; exports to parquet, with optional de-identification. |
| [`physiosim/`](HL7.md) | Python | A simulator that generates HL7 messages and sends them over MLLP, for development and tests. |
| [`specs/`](specs/README.md) | | The HL7 message schema, transport and parameter reference. |
| [`hsdi-bridge/`](hsdi-bridge/README.md) | Go | `hsdi2nats`: connects to HSDI gateways and publishes every message they send to JetStream, decoding nothing. Also `fakegw`, a fake gateway for testing without one, and `hsdi-report`, a structure report of the raw stream with patient values withheld. |
| [`hsdi-decode/`](hsdi-decode/README.md) | Go | `hsdi-decode`: decodes the raw stream, joining frames to their descriptors and patients, and publishes one protobuf message per frame. |
| `quack-bench/`, `benchmark.py`, `layout_bench.py` | Python | Benchmarks behind the HL7 storage decisions (`docs/decisions.md`). |

[`HL7.md`](HL7.md) describes the HL7 pipeline end to end: architecture, a demo that runs everything locally, deployment and tests. The HSDI pipeline is described in [`hsdi-bridge/README.md`](hsdi-bridge/README.md), which also defines the raw stream that [`hsdi-decode`](hsdi-decode/README.md) reads.

## Requirements

- [NATS server](https://nats.io/download/) with JetStream (`nats-server -js`)
- Go 1.26 or later for `mllp-bridge`, 1.27 or later for `hsdi-bridge` and `hsdi-decode`
- Python 3.14 or later, with [uv](https://docs.astral.sh/uv/), for the HL7 pipeline's Python parts

## Quick start

HL7, with simulated data (details in [`HL7.md`](HL7.md)):

```bash
cd mllp-bridge && go build -o mllp2nats ./cmd/mllp2nats && cd ..
python demo.py
```

HSDI, with the fake gateway, in three terminals (details in [`hsdi-bridge/README.md`](hsdi-bridge/README.md)):

```bash
nats-server -js
cd hsdi-bridge && go run ./cmd/fakegw -listen 127.0.0.1:12007 -sessions 4
cd hsdi-bridge && go run ./cmd/hsdi2nats -gateway FAKE=127.0.0.1:12007
```

Then, in a fourth, decode what the bridge publishes. The decoded stream reserves 50 GiB by default, more than a workstation's NATS usually allows, so cap it:

```bash
cd hsdi-decode && go run ./cmd/hsdi-decode -max-bytes 1073741824
```

## Tests

```bash
cd mllp-bridge && go test ./...
cd hsdi-bridge && go test ./...      # includes end-to-end tests with an in-process NATS server
cd hsdi-decode && go test ./...
cd physiomon && uv run --extra test python -m pytest ../test.py -v
```

The Python integration tests need NATS and a running `mllp2nats`, and are skipped without them.
