# IHE PCD Aggregator HL7 Transport Configuration

This document describes the connection layer, transport protocol, acknowledgment
behavior, and timing characteristics for receiving HL7 messages from an
IHE PCD-compliant physiological monitor aggregator.

---

## Feed Types

The aggregator supports three feed types, each with a distinct HL7 profile:

| Feed | HL7 Message | Description |
|---|---|---|
| **DEC** (Device Enterprise Communication) | `ORU^R01` (PCD-01) | Numeric metrics at scheduled intervals |
| **ACM** (Alarm Communication Management) | `ORU^R40` (PCD-04) | Alarm notifications |
| **WCM** (Waveform Content Module) | `ORU^R01` (PCD-01 + waveform OBRs) | Numeric vitals plus waveform data combined |

The sample files in this repository represent the **WFM** variant of WCM.

---

## Transport: TCP/IP with MLLP

Messages are delivered over a persistent TCP/IP socket connection. MLLP (Minimal
Lower Layer Protocol) framing is enabled by default and wraps each HL7 message with
start-of-block (`0x0B`) and end-of-block + carriage-return (`0x1C 0x0D`) bytes.

**Connection model:** The aggregator acts as the sender/client. Your
receiving system must listen on a configured TCP port. The aggregator connects
outbound to your host and port.

**Multiple outbound configs:**
- DEC feed supports up to 3 simultaneous outbound delivery configurations
- ACM feed supports up to 2 simultaneous outbound delivery configurations

---

## Acknowledgment (ACK)

An HL7 ACK message is expected for each received message by default. The aggregator
will wait for the ACK before sending the next message. Failure to ACK will stall the
feed.

**Note:** This behavior is configurable on the aggregator side per feed via the
`Wait For ACK` setting (default: `true`) and `ACKWaitTime` timeout (default:
3000 ms). If `Wait For ACK` is disabled, the aggregator sends messages without
waiting for an acknowledgment.

MSH-15 in outgoing messages is set to `AL` (always send accept ACK) and MSH-16 to
`NE` (no application-level ACK required beyond the transport-layer accept ACK).

---

## WCM/WFM Feed Timing

- **Feed frequency:** approximately 5.5 seconds between messages
- **Waveform window:** each waveform OBR covers a 6-second sample window
  (OBR-7 to OBR-8)
- **ForcedTimeDisadvantage:** default 1 second delay applied to message timestamps
  relative to actual capture time — accounts for processing latency
- **Timestamp jitter:** waveform channel timestamps have inherent jitter due to
  per-channel timing differences. Receiving systems must tolerate small discrepancies
  between OBR-7/OBR-8 timestamps across channels within the same message.
- **RoundingGranularity:** optional setting available to control how timestamps are
  rounded; check your aggregator configuration if sub-second precision matters.

---

## Message Backlog / Offline Buffering

The aggregator buffers messages when the receiving system is unavailable:
- Default backlog retention: 8 hours
- Maximum configurable backlog: 12 hours

Messages older than the retention window are discarded. Plan your receiver
availability and reconnect logic accordingly.

---

## Device Identification in Messages

The sending device is identified in several fields:

**MSH-3:** Sending application name (e.g. `WFM`)

**OBR-2 / OBR-3 (order numbers):**  
`<EUI64><timestamp>^<feed>^<EUI64>^EUI-64`  
The EUI-64 is the hardware identifier of the bridge device.

**OBX-18 (equipment instance):**  
`<instance>~<port>^<feed>^<EUI64>^EUI-64`

---

## Nomenclature

By default the aggregator uses **ISO/IEEE 11073-10101 "Rosetta" MDC nomenclature**
for all OBX-3 observation identifiers. Legacy nomenclature is configurable per feed
if required by your integration target.

---

## DEC Feed (Numeric Only)

The DEC feed delivers only numeric metrics without waveform data, using standard
PCD-01 format. Message structure is simpler: a single OBR section per message using
the `182777000^monitoring of patient^SCT` service code (same as the vitals OBR|1
in WCM/WFM messages).

Delivery interval is configurable (e.g. every 5 seconds, every 15 seconds).

---

## ACM Feed (Alarms)

The ACM feed uses PCD-04 (`ORU^R40`) for alarm notifications. It operates
independently of DEC and WCM. Alarm messages include alarm type, priority, and
the relevant patient/device context.

Up to 2 outbound ACM delivery targets are supported simultaneously.
