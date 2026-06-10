// mllp2nats: receives MLLP HL7 messages and publishes to NATS JetStream
//
// Usage:
//
//	mllp2nats.exe
//	mllp2nats.exe --port 2575 --nats nats://localhost:4222 --stream HL7 --subject hl7.inbound
//	mllp2nats.exe --ack        # enable MLLP ACKs (disabled by default)
//	mllp2nats.exe --log-level debug
//
// Run as a Windows service via Shawl:
//
//	shawl add --name mllp2nats --log-dir C:\logs\mllp2nats --log-rotate 5 --log-rotate-bytes 52428800 -- C:\services\mllp2nats.exe
package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// MLLP framing bytes (per HL7 spec)
const (
	startBlock     = 0x0B
	endBlock       = 0x1C
	carriageReturn = 0x0D
)

// ---- MLLP framing -------------------------------------------------------

func readMLLP(r io.Reader) ([]byte, error) {
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

func writeMLLP(w io.Writer, msg []byte) error {
	frame := make([]byte, 0, len(msg)+3)
	frame = append(frame, startBlock)
	frame = append(frame, msg...)
	frame = append(frame, endBlock, carriageReturn)
	_, err := w.Write(frame)
	return err
}

// buildACK returns a minimal HL7 AA acknowledgement with the sender's MSH-10
// control ID echoed in MSA-2, as required by the HL7 spec and aggregator.
func buildACK(msg []byte) []byte {
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

func handleConn(conn net.Conn, js jetstream.JetStream, subject string, sendACK bool, logger *slog.Logger) {
	defer conn.Close()
	remote := conn.RemoteAddr().String()
	logger.Info("connection accepted", "remote", remote)
	defer logger.Info("connection closed", "remote", remote)

	// Keep-alive: prevents silent drops on idle connections
	if tc, ok := conn.(*net.TCPConn); ok {
		tc.SetKeepAlive(true)
		tc.SetKeepAlivePeriod(3 * time.Minute)
	}

	for {
		msg, err := readMLLP(conn)
		if err != nil {
			if err != io.EOF {
				logger.Error("read error", "remote", remote, "err", err)
			}
			return
		}

		logger.Debug("message received", "remote", remote, "bytes", len(msg))

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err = js.Publish(ctx, subject, msg)
		cancel()
		if err != nil {
			logger.Error("jetstream publish failed", "subject", subject, "err", err)
			// Don't ACK on publish failure — let sender retry
			return
		}

		logger.Debug("message published", "subject", subject, "bytes", len(msg))

		if sendACK {
			if err := writeMLLP(conn, buildACK(msg)); err != nil {
				logger.Error("ack write error", "remote", remote, "err", err)
				return
			}
		}
	}
}

// ---- Main ---------------------------------------------------------------

func main() {
	port := flag.Int("port", 2575, "TCP port to listen on")
	natsURL := flag.String("nats", nats.DefaultURL, "NATS server URL")
	stream := flag.String("stream", "HL7", "JetStream stream name (must exist)")
	subject := flag.String("subject", "hl7.inbound", "JetStream subject to publish to")
	sendACK := flag.Bool("ack", false, "Send MLLP ACKs back to sender (default: disabled)")
	logLevel := flag.String("log-level", "info", "Log level: debug, info, warn, error")
	flag.Parse()

	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		fmt.Fprintf(os.Stderr, "invalid --log-level %q: must be debug, info, warn, or error\n", *logLevel)
		os.Exit(1)
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	// NATS connection with reconnect
	nc, err := nats.Connect(*natsURL,
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1), // reconnect forever
		nats.ReconnectWait(2*time.Second),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			logger.Warn("nats disconnected", "err", err)
		}),
		nats.ReconnectHandler(func(_ *nats.Conn) {
			logger.Info("nats reconnected")
		}),
	)
	if err != nil {
		logger.Error("nats connect failed", "err", err)
		os.Exit(1)
	}
	defer nc.Drain()

	js, err := jetstream.New(nc)
	if err != nil {
		logger.Error("jetstream init failed", "err", err)
		os.Exit(1)
	}

	addr := fmt.Sprintf(":%d", *port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Error("listen failed", "addr", addr, "err", err)
		os.Exit(1)
	}
	defer ln.Close()

	logger.Info("mllp2nats started",
		"port", *port,
		"nats", *natsURL,
		"stream", *stream,
		"subject", *subject,
		"ack", *sendACK,
	)

	// Graceful shutdown on SIGINT/SIGTERM (Shawl sends CTRL+C / SIGTERM)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		logger.Info("shutdown signal received")
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				logger.Info("listener closed, exiting")
				return
			default:
				logger.Error("accept error", "err", err)
				continue
			}
		}
		go handleConn(conn, js, *subject, *sendACK, logger)
	}
}
