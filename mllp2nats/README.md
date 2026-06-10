# mllp2nats

A minimal Windows-friendly Go service that receives HL7v2 messages over MLLP (TCP port 2575)
and publishes them to a NATS JetStream stream.

Inspired by the [GoogleCloudPlatform/mllp](https://github.com/GoogleCloudPlatform/mllp) adapter,
but with no GCP dependencies — one external dependency (nats.go).

---

## Prerequisites

- [Go 1.22+](https://go.dev/dl/)
- A running NATS server with JetStream enabled
- [Shawl](https://github.com/mtkennerly/shawl/releases) for Windows service wrapping

---

## Build

```bat
go mod tidy
go build -o mllp2nats.exe .
```

Cross-compile from Linux/macOS:

```bash
GOOS=windows GOARCH=amd64 go build -o mllp2nats.exe .
```

---

## Usage

```
mllp2nats.exe [flags]

Flags:
  --port        int     TCP port to listen on (default 2575)
  --nats        string  NATS server URL (default nats://127.0.0.1:4222)
  --stream      string  JetStream stream name — must already exist (default HL7)
  --subject     string  JetStream subject to publish to (default hl7.inbound)
  --ack                 Send MLLP ACKs back to sender (default: disabled)
  --log-level   string  Log level: debug, info, warn, error (default info)
```

### Example

```bat
mllp2nats.exe --port 2575 --nats nats://localhost:4222 --stream HL7 --subject hl7.inbound
```

ACKs are disabled by default. Enable with `--ack` when the sending system requires them.

---

## NATS JetStream Setup

The stream must exist before starting the service. Create it with the NATS CLI:

```bash
nats stream add HL7 \
  --subjects "hl7.>" \
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

Or let `service.py` create it automatically on first connect.

---

## Windows Service (Shawl)

[Download Shawl](https://github.com/mtkennerly/shawl/releases) and place it on your PATH.

**Register:**
```bat
shawl add --name mllp2nats ^
  --log-dir C:\logs\mllp2nats ^
  --log-rotate 5 ^
  --log-rotate-bytes 52428800 ^
  -- C:\services\mllp2nats\mllp2nats.exe ^
  --port 2575 ^
  --nats nats://localhost:4222 ^
  --stream HL7 ^
  --subject hl7.inbound
```

**Start / Stop / Remove:**
```bat
sc start mllp2nats
sc stop  mllp2nats
shawl remove --name mllp2nats
```

Shawl captures stdout and writes it to rotating log files in `--log-dir`.
`--log-rotate 5 --log-rotate-bytes 52428800` keeps up to 5 files at 50 MB each.

### Multiple ports / interfaces

Run one service instance per port:

```bat
shawl add --name mllp2nats-dec --log-dir C:\logs\mllp2nats-dec --log-rotate 5 --log-rotate-bytes 52428800 -- C:\services\mllp2nats.exe --port 2575 --subject hl7.inbound.dec
shawl add --name mllp2nats-acm --log-dir C:\logs\mllp2nats-acm --log-rotate 5 --log-rotate-bytes 52428800 -- C:\services\mllp2nats.exe --port 2576 --subject hl7.inbound.acm
shawl add --name mllp2nats-wcm --log-dir C:\logs\mllp2nats-wcm --log-rotate 5 --log-rotate-bytes 52428800 -- C:\services\mllp2nats.exe --port 2577 --subject hl7.inbound.wcm
```

---

## ACK Behaviour

- **Default (no ACK)**: connection stays open after each publish; the sender receives no reply. Use when the sending system has `Wait For ACK` disabled.
- **`--ack`**: sends a minimal HL7 2.3 `MSA|AA` ACK after each successful JetStream publish, with MSH-10 (message control ID) echoed in MSA-2.
- **Publish failure**: connection is closed regardless of `--ack` so the sender can retry.

---

## Logging

Structured JSON to stdout. Per-message logs (`message received`, `message published`) are
at `DEBUG` level and are suppressed at the default `info` level. Connection open/close and
errors are always logged at `INFO`/`ERROR`.

```json
{"time":"2026-05-26T10:00:00Z","level":"INFO","msg":"mllp2nats started","port":2575,"nats":"nats://localhost:4222","stream":"HL7","subject":"hl7.inbound","ack":false}
{"time":"2026-05-26T10:00:01Z","level":"INFO","msg":"connection accepted","remote":"10.0.0.5:49201"}
{"time":"2026-05-26T10:00:05Z","level":"INFO","msg":"connection closed","remote":"10.0.0.5:49201"}
```

Enable per-message logging with `--log-level debug`.

---

## Design Notes

- One goroutine per accepted TCP connection; the main goroutine only accepts.
- TCP keep-alive set to 3 minutes (prevents silent drops on idle connections).
- NATS reconnects indefinitely with 2s backoff; publishes fail fast (5s timeout) during outages.
- No GCP dependencies. Single non-stdlib import: `github.com/nats-io/nats.go`.
