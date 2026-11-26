# hsdi-decode

hsdi-decode decodes what `../hsdi-bridge` publishes. It reads the raw and state streams that the bridge's README describes ("The raw stream", "The state stream"); that README is the contract between the two. It shares no code with the bridge: the few subject and header names it needs are copied in `decode/rawstream.go`.

**Everything it reads and writes is PHI.**

## Packages and programs

| Path | What it is |
|---|---|
| `xbin` | Reads descriptors and decodes frames from the descriptor alone. Every binary type in the manual is sized, the ones the gateway uses are decoded, and unknown elements are skipped (manual 4-35 to 4-46, 5-2). An element with `BT` that contains elements with `BT`, as ventilator descriptors have, is read before them, in document order. It knows no signals: names, units, scales and invalid values come from the descriptor. |
| `session` | Keeps each gateway's sessions from session updates: patient identifier, location, admit and network state. |
| `decode` | Joins frames to descriptors and sessions, and runs the decoder against NATS. |
| `cmd/hsdi-decode` | The decoder program. |
| `proto/decoded.proto` | The decoded message format. |

Build with Go 1.24 or later:

```
go build ./cmd/...
GOOS=windows GOARCH=amd64 go build -o dist/windows-amd64/ ./cmd/...
go test ./...      # includes end-to-end tests with an in-process NATS server
```

## The decoder

```
hsdi-decode -nats nats://127.0.0.1:4222
```

`hsdi-decode` reads frames, descriptors and session updates from `HSDI_RAW` and publishes each decoded frame to `HSDI_DECODED`, on `hsdi.decoded.<gateway>.wav` or `hsdi.decoded.<gateway>.num`. **The decoded stream carries PHI:** patient identifiers, locations and real times.

- **Format.** The message format is `proto/decoded.proto`, protobuf, one message per frame. It is the first candidate in a comparison of formats and may be replaced.
  - **What a frame carries:** the gateway, session ID, format ID, block sequence number, the frame's own start time, when the bridge read it, the block rate, and its sequence number in `HSDI_RAW`.
  - **What each signal carries:** a name made of its path and qualifiers, such as `ecg/wav/ecgWaveformMXG/ecgWaveform lead=II index=1`, and its attributes, unit, scale and offset. It also carries the descriptor's invalid values and enum tokens.
  - **Values:** raw, before scaling. Arrays of `xs:short` are packed little-endian int16 bytes; everything else is a list of integers or floats.
- **Patient.** Each frame carries the patient and location from the latest session update for its session: the primary identifier, the unidentified flag, unit and bed, admit status and network state.
  - The location is the patient's visit location, or the device's when there is none. That choice is a judgement.
  - A frame whose session no update has named has no patient, and is counted as `NoSession`.
- **Descriptors.** Each frame is decoded with the latest descriptor for its session and format ID. A session's descriptors are dropped when a session update removes it.
- **Where it starts.** The decoder reads the raw stream with a durable consumer, `-consumer hsdi-decode`.
  - When the consumer is created, `-start` says where it begins: `new` (the default), `all`, or a duration such as `2h`. After that the consumer remembers its place, and `-start` is ignored. Delete the consumer to start again: `nats consumer rm HSDI_RAW hsdi-decode -f`.
  - Before its first raw message, the decoder reads from `HSDI_STATE` the session updates and descriptors received up to that message, so the first frames already have their patient and descriptor.
- **Restarts.** Raw messages are acknowledged only after their decoded frames are stored. After a crash the decoder starts from the first raw message not acknowledged. Decoded messages have `Nats-Msg-Id` set to the raw sequence number, and the stream drops duplicates for 2 minutes, so a restart within 2 minutes repeats nothing. After a longer gap a few frames can appear twice; the `raw_seq` field tells them apart.
- **Stream.** `HSDI_DECODED` is created if it does not exist: file storage, S2 compression, 24-hour maximum age (`-max-age`) and at most 50 GiB counted before compression (`-max-bytes`), whichever is reached first. The raw stream keeps 7 days, so decoding can be run again from it.
- **Size.** A decoded frame is about 3.4 times its raw frame: 3,205 bytes for a 941-byte waveform frame with seven ECG leads, three markers and a pleth. 2,166 of those bytes are signal metadata (name, attributes, unit, scale, invalid values) repeated in every frame. At 160 GB of raw frames a day, for example, a day of decoded frames would be about 540 GB before compression, and the 50 GiB default holds about two hours. Measured on one frame shape in a test.
- **Counters** are logged every `-stats-interval`, including how many raw messages are waiting (`rawPending`).

### Starting again

After a breaking change to the raw message format, or to start decoding from scratch:

```
nats stream purge HSDI_DECODED -f
nats consumer rm HSDI_RAW hsdi-decode -f
```
