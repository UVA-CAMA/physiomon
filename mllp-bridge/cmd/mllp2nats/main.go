// mllp2nats: receives MLLP HL7 messages and publishes to NATS JetStream
//
// Usage:
//
//	mllp2nats.exe                                   # one listener on :2575
//	mllp2nats.exe --listen DEC=:2575 --listen ACM=:2576 --listen WCM=:2577
//	mllp2nats.exe --nats nats://localhost:4222 --stream HL7 --subject hl7.inbound
//	mllp2nats.exe --ack        # enable MLLP ACKs (disabled by default)
//	mllp2nats.exe --log-level debug
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/UVA-CAMA/physiomon/mllp-bridge/internal/mllp"
)

// ---- Main ---------------------------------------------------------------

func main() {
	var listen mllp.ListenFlag
	flag.Var(&listen, "listen", "feed to listen on, as name=addr such as WCM=:2577; repeat for more (default "+mllp.DefaultAddr+")")
	natsURL := flag.String("nats", nats.DefaultURL, "NATS server URL")
	stream := flag.String("stream", "HL7", "JetStream stream name (must exist)")
	subject := flag.String("subject", "hl7.inbound", "JetStream subject to publish to")
	sendACK := flag.Bool("ack", false, "Send MLLP ACKs back to sender (default: disabled)")
	logLevel := flag.String("log-level", "info", "Log level: debug, info, warn, error")
	statsEvery := flag.Duration("stats-interval", time.Minute, "how often to log each feed's counters; 0 to disable")
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

	publish := func(msg []byte) error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := js.Publish(ctx, *subject, msg)
		return err
	}

	feeds, err := mllp.Listen(listen)
	if err != nil {
		logger.Error("listen failed", "err", err)
		os.Exit(1)
	}

	logger.Info("mllp2nats started",
		"listen", listen.String(),
		"nats", *natsURL,
		"stream", *stream,
		"subject", *subject,
		"ack", *sendACK,
	)

	// Graceful shutdown on SIGINT/SIGTERM (WinSW sends CTRL+C)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	mllp.Serve(ctx, feeds, func() func([]byte) error { return publish }, *sendACK, *statsEvery, logger)
	logger.Info("listeners closed, exiting")
}
