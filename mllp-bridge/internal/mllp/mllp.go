// Package mllp listens for HL7 messages framed with MLLP and hands each one
// to a publisher. It is shared by the programs in cmd/.
package mllp

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync/atomic"
	"time"
)

// MLLP framing bytes (per HL7 spec)
const (
	startBlock     = 0x0B
	endBlock       = 0x1C
	carriageReturn = 0x0D
)

// ---- MLLP framing -------------------------------------------------------

// Read returns the next message from r, without its framing bytes.
func Read(r io.Reader) ([]byte, error) {
	// Read until start block
	buf := make([]byte, 1)
	for {
		_, err := io.ReadFull(r, buf)
		if err != nil {
			return nil, err
		}
		if buf[0] == startBlock {
			break
		}
	}

	var msg []byte
	frame := make([]byte, 1)
	for {
		_, err := io.ReadFull(r, frame)
		if err != nil {
			return nil, err
		}
		if frame[0] == endBlock {
			// Consume the required trailing carriage return
			_, err := io.ReadFull(r, frame)
			if err != nil {
				return nil, fmt.Errorf("reading trailing CR after end block: %w", err)
			}
			if frame[0] != carriageReturn {
				return nil, fmt.Errorf("expected CR after end block, got 0x%s", hex.EncodeToString(frame))
			}
			return msg, nil
		}
		msg = append(msg, frame[0])
	}
}

// Write writes msg to w with MLLP framing.
func Write(w io.Writer, msg []byte) error {
	frame := make([]byte, 0, len(msg)+3)
	frame = append(frame, startBlock)
	frame = append(frame, msg...)
	frame = append(frame, endBlock, carriageReturn)
	_, err := w.Write(frame)
	return err
}

// BuildACK returns a minimal HL7 AA acknowledgement with the sender's MSH-10
// control ID echoed in MSA-2, per the HL7 spec. Only sent with --ack; field
// installations run with the aggregator's Wait For ACK disabled.
func BuildACK(msg []byte) []byte {
	var ctrlID string
	// Normalise LF → CR so both aggregator (\n) and spec-compliant (\r) messages work.
	normalised := bytes.ReplaceAll(msg, []byte("\n"), []byte("\r"))
	for _, seg := range bytes.Split(normalised, []byte("\r")) {
		if bytes.HasPrefix(seg, []byte("MSH")) {
			fields := bytes.SplitN(seg, []byte("|"), 11)
			if len(fields) > 9 {
				ctrlID = string(fields[9])
			}
			break
		}
	}
	return []byte("MSH|^~\\&|MLLP2NATS||SENDER||" +
		time.Now().Format("20060102150405") +
		"||ACK||P|2.3\rMSA|AA|" + ctrlID + "\r")
}

// ---- Connection handler -------------------------------------------------

// handle reads messages from conn until it closes, and passes each to
// publish. If publish returns an error, handle sends no ACK and closes the
// connection, so the sender can retry. With sendACK, each message is ACKed
// after publish returns, so publish must return only once the message is
// stored.
func handle(conn net.Conn, publish func(msg []byte) error, sendACK bool, logger *slog.Logger, c *counters) {
	defer conn.Close()
	remote := conn.RemoteAddr().String()
	logger.Info("connection accepted", "remote", remote)
	defer logger.Info("connection closed", "remote", remote)
	c.open.Add(1)
	defer c.open.Add(-1)

	// Keep-alive: prevents silent drops on idle connections
	if tc, ok := conn.(*net.TCPConn); ok {
		tc.SetKeepAlive(true)
		tc.SetKeepAlivePeriod(3 * time.Minute)
	}

	// Buffered, because Read takes one byte at a time. bytesRead counts every
	// byte received, framed or not: if it rises while messages does not, the
	// sender is not sending MLLP framing.
	r := bufio.NewReaderSize(countingReader{conn, &c.bytesRead}, 64<<10)
	for {
		msg, err := Read(r)
		if err != nil {
			if err != io.EOF {
				logger.Error("read error", "remote", remote, "err", err)
			}
			return
		}

		logger.Debug("message received", "remote", remote, "bytes", len(msg))

		if err := publish(msg); err != nil {
			c.failed.Add(1)
			logger.Error("publish failed", "remote", remote, "err", err)
			// Don't ACK on publish failure — let sender retry
			return
		}
		c.messages.Add(1)
		c.bytes.Add(int64(len(msg)))

		logger.Debug("message published", "remote", remote, "bytes", len(msg))

		if sendACK {
			if err := Write(conn, BuildACK(msg)); err != nil {
				logger.Error("ack write error", "remote", remote, "err", err)
				return
			}
		}
	}
}

// countingReader adds the bytes it reads to n.
type countingReader struct {
	r io.Reader
	n *atomic.Int64
}

func (c countingReader) Read(p []byte) (int, error) {
	k, err := c.r.Read(p)
	c.n.Add(int64(k))
	return k, err
}
