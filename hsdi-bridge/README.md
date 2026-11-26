# hsdi-bridge

`hsdi2nats`, the bridge, connects to gateways over HSDI, the High Speed Data Interface (TCP port 2007). It publishes every MIME part the gateways send, unchanged, to a NATS JetStream stream. It does not decode anything. A separate decoder, `../hsdi-decode`, reads that stream.

Page numbers in parentheses, such as (3-19), refer to the HSDI reference manual.

**The raw stream contains PHI.** Session updates carry patient names and identifiers. Every message carries dates. Logs do not: they print only the device part of a session ID, without its start time.

## Programs

| Program | What it does |
|---|---|
| `hsdi2nats` | The bridge. One process can serve several gateways. |
| `fakegw` | A fake gateway with made-up data and fault injection, for running the bridge without a real gateway. |
| `hsdi-report` | Reads the raw stream and prints a structure report with patient values withheld, to review before sharing. |

The decoder is a separate module, `../hsdi-decode`. It reads the streams described below and shares no code with the bridge.

Build with Go 1.27 or later:

```
go build ./cmd/...
GOOS=windows GOARCH=amd64 go build ./cmd/...     # Windows executables
go test ./...                                      # includes end-to-end tests with an in-process NATS server
```

## Running

```
hsdi2nats -gateway ICU=10.0.0.5:2007 -gateway ED=10.0.0.6 -nats nats://127.0.0.1:4222
```

- **`-gateway name=host[:port]`**: the name becomes part of the subjects, `hsdi.raw.<name>.>`. Repeat for more gateways. Names are case-sensitive.
- **Streams:** by default the bridge creates two streams if they don't exist, both with file storage and S2 compression. An existing stream is never changed. `-create-stream=false` turns creation off.
  - `HSDI_RAW`, subjects `hsdi.raw.>`, everything, 7-day maximum age. Change with `-stream`, `-max-age`, `-max-bytes`.
  - `HSDI_STATE`, subjects `hsdi.state.>`, copies of descriptors, session updates and events, no age limit. Change with `-state-stream`, `-state-max-bytes`.
- **Status:** `-http 127.0.0.1:8090` serves `/healthz` and `/status` (counters as JSON). `-http ""` turns them off.
- **Counter logging:** counters are logged every `-stats-interval` (default one minute).
- **Shutdown:** Ctrl-C or SIGTERM ends the streams and session updates with the gateway, naming only the requests the gateway accepted, waits up to 3 seconds for its replies, flushes to NATS and exits.

To try it without a gateway, in three terminals:

```
nats-server -js
fakegw -listen 127.0.0.1:12007 -sessions 4 -extra-headers 12 -binend-after 40
hsdi2nats -gateway FAKE=127.0.0.1:12007
```

### Emptying the streams

After a breaking change to the raw message format, stop the bridge, empty both streams, deploy and start it again. Purging keeps each stream's settings, such as its size limit. A decoder reading the streams has to be reset too; its README says how.

```
nats stream purge HSDI_RAW -f
nats stream purge HSDI_STATE -f
```

## What the bridge does on the connection

1. **Connect and hello.** It sends `<hello>`. If the reply is a nak, or no reply arrives in 10 seconds, it disconnects and backs off. The client session ID is built from the gateway's IPv4 address, port and Unix seconds, which the gateways accept.
2. **Subscribe for all sessions.** It sends one session update request and two stream requests, for numerics and configuration (`//params/*/(num|cfg)/*`) and for waveforms (`//params/*/wav/*`), the two queries the manual lists (3-19).
3. **Acknowledge.** It replies `<genericResponse>` to every `sessionUpdate`, `binHeader`, `binDescriptor`, `binGapStart`, `binGapEnd` and `binEnd` (manual 3-15, 3-23 to 3-26). It also answers keep-alive requests.
4. **Re-request after `<binEnd>`.** After `-rerequest-delay` (5 seconds), it requests that session's stream again (3-25), unless the reason says the session was "removed from data acquisition" or the stream ended through "unsubscription". Real gateways refuse every re-request after "removed", and new sessions join the all-sessions subscription without one. Other reasons, such as "Client too slow", are still re-requested.
5. **Keep-alive.** After `-keepalive-idle` (30 seconds) with nothing read, it sends `<keepAliveRequest>`. If there's no reply within `-keepalive-timeout` (10 seconds), it reconnects. The manual says the gateway v1 never sends keep-alives itself (3-27); the real gateways do, and the bridge answers them.
6. **Reconnect** with backoff from 1 second to `-backoff-max` (1 minute). From the fifth attempt in a row that does not complete hello, each failure is logged as an error, `gateway not answering`, with the count and how long the gateway has been down. While a gateway is not connected, `/status` shows `failedAttempts` and `downSince` for it. A gateway has been seen to stop answering a keep-alive, then accept connections but ignore hello for about an hour, then recover without intervention. The cause is not established. `fakegw -ignore-hello` reproduces the behaviour.
7. **Late responses.** A response arriving more than 2 seconds after its request is logged and counted as late. The connection is kept.

**MIME framing:**
- There is no limit on header lines. The most seen and the number of parts over ten are in the counters (`maxHeaderLines`, `overTenHeaderLines` at `/status`).
- Each part is read by its `Content-Length`. Boundaries are compared without regard to case.
- A message may begin with any of its top-level headers (`Mime-Version`, `Content-Type`, `Content-Length`).
- **XML without MIME framing.** The real gateways send `<keepAliveRequest>` as a bare XML document between MIME messages, about once a minute per connection while the bridge answers them, though the manual says the gateway never sends keep-alives (3-27) and that every message is MIME-framed. A line starting `<?xml` between messages begins such a document, read up to the line that closes its root element. It is published like any XML part and answered. Counted as `bareMessages` at `/status`.
- After a framing error, the bridge skips to the next line that starts a message. It publishes any complete parts from the broken message and counts the bytes it skipped.

**Back-pressure.** If NATS stops acknowledging, the bridge stops reading from the gateway once `-max-pending` publishes (4,000) are waiting. The gateway may then end streams as "Client too slow". The bridge requests them again, and the stall shows in the counters.

## The raw stream

Each message is one MIME part, with the part body as the payload. Subjects name the gateway, direction and message type. Defined in `internal/raw`.

| Subject | Messages |
|---|---|
| `hsdi.raw.<gateway>.in.binary` | binary frames |
| `hsdi.raw.<gateway>.in.<type>` | XML from the gateway, such as `in.binDescriptor`, `in.sessionUpdate`; `in.xml` if the header could not be read |
| `hsdi.raw.<gateway>.in.discarded` | bytes skipped while resynchronising |
| `hsdi.raw.<gateway>.out.<type>` | XML the bridge sent, such as `out.getStreamRequest` |
| `hsdi.raw.<gateway>.event.<type>` | `event.connected`, `event.disconnected`, `event.framing-error` |

A stream numbers messages across all its subjects, so a consumer of `hsdi.raw.<gateway>.>` gets one gateway's messages in the order the bridge read them.

Headers carry only what the subject and body don't already say:

| Header | Value |
|---|---|
| `Hsdi-Conn` | every message. Connection ID, such as `20261108T142512Z-1`, new for each TCP connection. Unique within a gateway only: gateways connected in the same second have the same IDs. |
| `Hsdi-Seq` | every message. 1, 2, 3, ... within the connection. No gaps unless a publish failed; failures are logged and counted. |
| `Hsdi-Received` | every message. When the bridge read the MIME message, RFC 3339 UTC. Sent messages and events carry the time they were written. |
| `Content-Type` | only if the gateway's content type is not the usual one: `application/x-sapphire+binary` for `in.binary`, `application/x-sapphire+xml` for XML. |
| `Hsdi-Part` | only if the MIME message had more than one part: `n/m`, part n of m. |
| `Hsdi-Discarded` | `in.discarded` only: the number of bytes skipped. |

`in.discarded` messages hold the bytes the bridge skipped while looking for the start of a message, up to 1 MB, one line per line read. `hsdi-report -discarded` prints them. That output is raw gateway content: look at it inside the PHI boundary only.

Event bodies are JSON: `connected` has `remote`; `disconnected` and `framing-error` have `reason`. A decoder should treat `connected` as the start of a new conversation: the gateway sends new `binHeader` and descriptor messages on each connection.

There is no `Nats-Msg-Id`, so JetStream does not de-duplicate. The bridge only retries a publish that was never sent, so it cannot write a message twice.

## The state stream

The gateway sends a session's `binHeader` and descriptors once, when its stream starts, and a patient can stay connected for longer than the raw stream keeps messages. So the bridge also publishes a copy of every message needed to read later frames to `HSDI_STATE`, which has no age limit:

| Subject | Messages |
|---|---|
| `hsdi.state.<gateway>.binDescriptor.<session>.<channel>` | descriptors |
| `hsdi.state.<gateway>.binHeader.<session>.<channel>` | binHeaders |
| `hsdi.state.<gateway>.binEnd.<session>.<channel>` | stream ends |
| `hsdi.state.<gateway>.sessionUpdate` | session updates, whole |
| `hsdi.state.<gateway>.event.<type>` | connection events |

Copies have the raw message's body and headers, including `Hsdi-Conn` and `Hsdi-Seq`, so a reader can tell which descriptor applied to which frames. The bridge reads only the XML header, as for the raw stream. Nothing is pruned yet: on a busy gateway this is roughly 1 MB an hour before compression.

Binary frames don't carry their channel. A decoder joins them to descriptors by session ID and format ID, both of which are in the frame.

## The structure report

```
hsdi-report -gateway ICU -since 24h > icu-report.txt
```

The report prints:
- message counts and sizes by type
- bytes skipped while resynchronising
- bridge events and disconnect reasons
- each gateway's connections in order, as offsets from the start of the window, never dates: frames, keep-alives each way, and how the connection ended
- `binEnd` reasons
- frame counts, lengths and rates per channel
- every distinct descriptor shape. Descriptors come from the whole state stream, so `-since` windows still match frames to descriptors; `-state-stream ""` uses only descriptors in the window.
- signal paths compared with the manual's lists of waveforms, numerics and configuration settings (`internal/report/manual_paths.txt`): which the manual lists and were seen, which it lists and were not, and which were seen but not listed, with types, units and attributes the manual does not mention. Run it over long periods and all gateways: rare signals only show up over time.
- the element paths of session updates and other messages, with occurrence counts

Values are withheld and printed as `…` unless an allow rule in `internal/report/shape.go` names them:
- **Descriptors:** signal metadata attributes (`BT`, `asizeBT`, `S`, `U`, `offset`, `INV`, `mapINV`, `site`, `index`, `lead`, and so on). Also `label`, unless `-labels=false`; the manual says labels are usually set by staff (4-4).
- **Session updates:** status and type attributes, and the fixed-vocabulary `V` values (service, network, patientStatus, modelID, vendorID, manufacturerName, unknownID, primary). Names, identifiers, ages, locations, bed and unit names, and dates are withheld.
- **Message headers:** skipped entirely.
- **Session IDs:** never printed.

The tests check that the report contains none of the patient values in the sample messages. **The report is still meant to be read by a person inside the PHI boundary before it is shared.**

## What real gateways have shown


1. **Keep-alives.** The gateways send their own keep-alive requests as bare XML (see "MIME framing") and answer the bridge's. In a 65-minute run, 15 of the bridge's 16 keep-alives were answered.
2. **A gateway can stop answering hello.** One did not answer a keep-alive, then accepted 58 connections over 63 minutes without answering hello, then recovered by itself. The cause is not established. The bridge logs `gateway not answering` and shows `failedAttempts` and `downSince` at `/status` while this lasts.
3. **Re-requests after "removed from data acquisition"** were all refused with `nak_unknownSessionID`, and new sessions joined the all-sessions subscription without one, so the bridge no longer re-requests then. Other `binEnd` reasons have not been seen.
4. **Message sizes.** The largest session update seen was 333,504 bytes, within NATS's default 1 MB payload limit.

## Not yet established

1. **Channel numbers.** Which `msgCHN` the gateway expects on `endStreamRequest`. The manual's examples use 1. The bridge uses 0.
2. **JetStream not ready.** The bridge exits if JetStream does not answer at start-up, as after a server restart while JetStream recovers its streams. Run as a service that restarts it, or start it once `nats account info` lists the streams.
