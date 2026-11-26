# physiomon: the HL7 pipeline

Pipeline for ingesting, routing, and storing patient physiological data from
HL7 v2 / MLLP physiological monitor aggregators implementing IHE PCD profiles
(PCD-01, PCD-04).

## Architecture

```
Monitor aggregator  (or mock generator via physiosim/sender.py)
    │  HL7 v2 over MLLP (TCP)
    ▼
mllp2nats           Go — receives MLLP, publishes raw bytes to NATS JetStream
    │  NATS JetStream  stream: HL7   subject: hl7.inbound
    ▼
service.py          Python — subscribes, parses HL7, writes to DuckDB staging
```

Three feed types, one MLLP port each (DEC 2575, ACM 2576, WCM 2577), all
published to the same NATS subject:

| Feed | HL7 type | Content |
|------|----------|---------|
| DEC  | ORU^R01  | Numeric vitals — HR, IBP, SpO₂, temperature, ST segments |
| ACM  | ORU^R40  | Alarm lifecycle events |
| WCM  | ORU^R01  | Numeric vitals + 12-lead ECG waveforms at 240 Hz |

## Layout

```
mllp-bridge/  Go MLLP→NATS bridge, mllp2nats (Windows service in production)
physiomon/    Python consumer
  service.py    long-running NATS subscriber → DuckDB staging (production)
  parser.py     parse_vectorized: HL7 bytes → per-segment Polars DataFrames
  config.py     all endpoints; overridable via PHYSIOMON_* env vars
physiosim/    Python simulator (dev/test/demo only)
  sender.py     send HL7 via MLLP — example files or mock generator
  generator.py  mock HL7 message generators for all three feed types
  examples/     Reference HL7 sample files
specs/        Message schema, transport config, parameter reference
demo.py       start everything and send simulated data
test.py       unit + integration tests
```

## Prerequisites

- **NATS server** with JetStream — macOS: `brew install nats-server`,
  Windows: `winget install NATS.io.nats-server`
- **Go 1.26+** to build `mllp2nats`
- **Python 3.14+** with [uv](https://docs.astral.sh/uv/)

## Quick start (demo)

Build `mllp2nats` once, then use `demo.py` to start everything:

```bash
cd mllp-bridge && go build -o mllp2nats ./cmd/mllp2nats && cd ..
python demo.py                            # 1 patient, 20 msgs per feed
python demo.py --patients 2 --duration 30
python demo.py --help                     # full sender.py option list
```

`demo.py` starts `nats-server`, `mllp2nats`, and `service.py` if they aren't
already running, and stops what it started on exit.

Staging output lands in `physiomon/out/staging/YYYYMMDD.duckdb`, one file per UTC day.

## Running manually

```bash
# Terminal 1 — infrastructure
nats-server -js
# Terminal 2
cd mllp-bridge && ./mllp2nats
# Terminal 3 — production consumer
cd physiomon && python service.py
# Terminal 4 — send data
cd physiosim && python sender.py --files "examples/*.hl7"  # example files
cd physiosim && python sender.py --count 20
```

## Production

On Windows, `mllp-bridge/run-all.bat` starts `mllp2nats` with the three feed
listeners (2575–2577) in one console window — no admin rights needed. To run it as a
Windows service, see [`mllp-bridge/README.md`](mllp-bridge/README.md).
`service.py` runs as a long-running process. Endpoints are configured via
environment variables; see `physiomon/config.py`.

### Deploy checklist

1. Copy `config.py`, `parser.py`, `service.py`, `export.py` and `export_deid.py` into
   the server's `physiomon` folder, `<physiomon-dir>`.
2. Set `PHYSIOMON_STAGING_DIR` to a folder on a large drive, `<staging-dir>`.
3. Start `service.py`. Check:
   - a `YYYYMMDD.duckdb` file appears for today (UTC);
   - the log shows no parse errors;
   - `progress.py` shows the backlog falling.
4. After an hour or two, record the day file's size per hour and plan disk use
   from it.
5. Optional, for live reads: before starting, set `PHYSIOMON_QUACK_LISTEN` to
   `quack:localhost:9494` and `PHYSIOMON_QUACK_TOKEN` to a secret of 4 or more
   characters. Quack needs two DuckDB extensions, `quack` and `httpfs`, on both the
   service and every reader.
   - **Without internet**, put both files in a shared folder, `<extensions-dir>`, in
     DuckDB's layout: `<extensions-dir>\v2.0.0\windows_amd64\quack.duckdb_extension`
     and `httpfs.duckdb_extension`. Download them from
     `https://extensions.duckdb.org/v2.0.0/windows_amd64/<name>.duckdb_extension.gz`
     and unzip.
   - Set `PHYSIOMON_DUCKDB_EXTENSION_DIR=<extensions-dir>`. Readers pass the same
     folder (below).
   - Then check that a live read works.
6. Day files older than yesterday are closed. You can read, move or archive them
   while the service runs. Late data reopens a file for one batch, and data for a
   moved day goes to `late/`.

### Example launcher (Windows)

`run_service.bat`, with Quack on and extensions in a shared folder. Replace the
`<…>` placeholders with your paths. The token must be 4+ characters with no `%`.

```bat
cd /d <physiomon-dir>
set PHYSIOMON_STAGING_DIR=<staging-dir>
set PHYSIOMON_DUCKDB_EXTENSION_DIR=<extensions-dir>
set PHYSIOMON_QUACK_LISTEN=quack:localhost:9494
set PHYSIOMON_QUACK_TOKEN=change-me
<python> service.py
```

`<python>` is the Python with the packages in `physiomon/pyproject.toml` installed.
The log should include `serving Quack on quack:localhost:9494`.

### Live reads (Quack)

With Quack on, query today's and yesterday's data while the service runs. Use any
DuckDB 2.0 client on the same machine, e.g. Python:

```python
import duckdb
con = duckdb.connect(config={"extension_directory": r"<extensions-dir>"})  # omit if installed per user
con.execute("LOAD quack")
con.execute("ATTACH 'quack:localhost:9494' AS live (TOKEN 'change-me')")
# Is ingest moving? Messages per feed, newest message time and arrival time
con.sql("SELECT feed, count(*), max(msg_ts), max(received_at) FROM live.msh_all GROUP BY feed").show()
```

The views are `msh_all`, `pv1_all`, `numerics_all`, `waveforms_all`, `signals_all`,
`alarms_all` and the other tables with `_all`. They cover the day files the service
has open: today and yesterday (UTC). Use
`quack_query('quack:localhost:9494', '<sql>', token := '<token>')` to run SQL on the
server side, e.g. against one day as `d20261118.numerics`. Rows appear when their
batch commits. Older days: attach the files directly with `(READ_ONLY)`.

## Tests

```bash
cd physiomon
uv run --extra test python -m pytest ../test.py -v
```

Unit tests run unconditionally. Integration tests require NATS and `mllp2nats`
and skip gracefully if absent.

See [`docs/decisions.md`](docs/decisions.md) for design rationale and
[`specs/README.md`](specs/README.md) for message schema and field reference.
