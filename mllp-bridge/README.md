# mllp-bridge

A minimal Windows-friendly Go program, `mllp2nats`, that receives HL7v2 messages over MLLP
and publishes each one, unchanged, to NATS JetStream (subject `hl7.inbound`). One process
listens on every feed's port, named with `--listen`: DEC on 2575, ACM on 2576, WCM on 2577.

The listeners, MLLP framing, ACK and connection handling are in `internal/mllp`.

Inspired by the [GoogleCloudPlatform/mllp](https://github.com/GoogleCloudPlatform/mllp) adapter,
but with no GCP dependencies.

---

## Prerequisites

- [Go 1.26+](https://go.dev/dl/)
- A running NATS server with JetStream enabled, and optionally the
  [NATS CLI](https://github.com/nats-io/natscli/releases) (`nats.exe`) to create the stream
- Optional: [WinSW](https://github.com/winsw/winsw/releases) for running as a Windows service (needs admin)

---

## Build

```bat
go build -o . ./cmd/...     # mllp2nats here, where demo.py looks
go test ./...
```

Release builds go in `dist/`, one folder per platform, with a copy of this README.
`dist/` is not in git. From Linux/macOS:

```bash
GOOS=windows GOARCH=amd64 go build -o dist/windows-amd64/ ./cmd/...
GOOS=linux GOARCH=amd64 go build -o dist/linux-amd64/ ./cmd/...
cp README.md dist/
```

---

## Usage

```
mllp2nats.exe [flags]

Flags:
  --listen      name=addr  Feed to listen on, such as WCM=:2577; repeat for each feed.
                           A bare addr is named after itself. (default :2575)
  --nats        string  NATS server URL (default nats://127.0.0.1:4222)
  --stream      string  JetStream stream name — must already exist (default HL7)
  --subject     string  JetStream subject to publish to (default hl7.inbound)
  --ack                 Send MLLP ACKs back to sender (default: disabled)
  --log-level   string  Log level: debug, info, warn, error (default info)
  --stats-interval dur  How often to log each feed's counters; 0 to disable (default 1m)
```

### Example

```bat
mllp2nats.exe --listen DEC=:2575 --listen ACM=:2576 --listen WCM=:2577 --nats nats://localhost:4222
```

Every port is opened at startup; if one cannot be, the program exits with `listen failed`.

ACKs are disabled by default. Field installations run the aggregator with `Wait For ACK`
off, so `--ack` is not expected to be needed (the spec docs describe ACK as the default,
but that doesn't match deployed sites).

---

## Quick start on Windows (no admin)

Start `nats-server -js` and create the `HL7` stream (below, or run `physiomon\service.py`
once), then put `mllp2nats.exe` next to `run-all.bat` and run it. It opens one console
window running `mllp2nats` with the three feeds, publishing to `hl7.inbound` at
`--log-level debug`. The window stays open after a crash so you can read the error.

---

## NATS JetStream Setup

The stream must exist before starting `mllp2nats`. Create it with the NATS CLI:

```bash
nats stream add HL7 \
  --subjects hl7.inbound \
  --storage file \
  --replicas 1 \
  --compression s2 \
  --retention limits \
  --discard old \
  --max-bytes 10GB \
  --defaults
```

| Flag | Value | Notes |
|---|---|---|
| `--retention limits` | limits | Drop messages when any limit is hit |
| `--discard old` | old | Evict oldest messages first when full |
| `--max-bytes` | 10 GiB | Hard cap on stream storage |
| `--compression` | s2 | S2/Snappy block compression — effective on repetitive HL7 text |

This matches what `service.py` creates automatically on first connect (`_ensure_stream`).

---

## Windows service (WinSW)

[WinSW](https://github.com/winsw/winsw) can run `mllp2nats.exe` as a service. Run the
executable directly, not through a .bat, so WinSW's Ctrl+C reaches it. WinSW reads the
XML file named like its own executable, so copy `WinSW-x64.exe` as, for example,
`mllp2nats-service.exe`, next to `mllp2nats.exe` and this `mllp2nats-service.xml`:

```xml
<service>
  <id>mllp2nats</id>
  <name>mllp2nats</name>
  <description>Receives HL7 over MLLP and publishes it to NATS JetStream.</description>
  <executable>%BASE%\mllp2nats.exe</executable>
  <arguments>--listen DEC=:2575 --listen ACM=:2576 --listen WCM=:2577 --nats nats://localhost:4222</arguments>
  <logpath>%BASE%\logs</logpath>
  <log mode="roll-by-size">
    <sizeThreshold>51200</sizeThreshold>
    <keepFiles>5</keepFiles>
  </log>
  <onfailure action="restart" delay="10 sec"/>
  <stoptimeout>15 sec</stoptimeout>
</service>
```

Then, as Administrator:

```bat
mllp2nats-service.exe install
sc start mllp2nats
sc stop  mllp2nats
mllp2nats-service.exe uninstall
```

---

## ACK Behaviour

- **Default (no ACK)**: connection stays open after each publish; the sender receives no reply. Use when the sending system has `Wait For ACK` disabled.
- **`--ack`**: sends a minimal HL7 2.3 `MSA|AA` ACK after each successful publish, with MSH-10 (message control ID) echoed in MSA-2.
- **Publish failure**: connection is closed regardless of `--ack` so the sender can retry.

---

## Logging

Structured JSON to stdout. Per-message logs (`message received`, `message published`) are
at `DEBUG` level and are suppressed at the default `info` level. Connection open/close and
errors are always logged at `INFO`/`ERROR`. Every line about a listener or connection
carries `feed`, the name given in `--listen`.

Every `--stats-interval` (1 minute), each feed logs its open connections, and since the
program started: messages, their bytes, failed publishes, and `bytesRead`, every byte
received whether or not it was framed. A feed whose `messages` stops rising has gone
quiet, even though the process is running. If `bytesRead` rises while `messages` does
not, the sender is sending something other than MLLP framing (`0x0B` … `0x1C 0x0D`);
anything before a start block is discarded.

```json
{"time":"2026-05-26T10:00:00Z","level":"INFO","msg":"listening","feed":"WCM","addr":"[::]:2577"}
{"time":"2026-05-26T10:00:01Z","level":"INFO","msg":"connection accepted","feed":"WCM","remote":"10.0.0.5:49201"}
{"time":"2026-05-26T10:01:00Z","level":"INFO","msg":"counters","feed":"WCM","connections":1,"messages":2410,"bytes":198734120,"publishFailed":0,"bytesRead":198741350}
{"time":"2026-05-26T10:00:05Z","level":"INFO","msg":"connection closed","feed":"WCM","remote":"10.0.0.5:49201"}
```

Enable per-message logging with `--log-level debug`.

---

## Design Notes

- One goroutine per listener accepts, and one per accepted TCP connection reads.
- TCP keep-alive set to 3 minutes (prevents silent drops on idle connections).
- Each connection is read through a 64 KB buffer; the framing reads one byte at a time.
- `mllp2nats`: NATS reconnects indefinitely with 2s backoff; publishes fail fast (5s timeout) during outages.
- No GCP dependencies. Single non-stdlib import: `github.com/nats-io/nats.go`.
