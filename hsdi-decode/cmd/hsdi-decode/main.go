// Command hsdi-decode reads binary frames from the raw stream, decodes them
// with their descriptors, adds the patient and location from the session
// updates, and publishes one message per frame to the decoded stream. The
// message format is proto/decoded.proto. The decoded stream carries PHI.
//
//	hsdi-decode -nats nats://127.0.0.1:4222
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/UVA-CAMA/physiomon/hsdi-decode/decode"
)

func main() {
	natsURL := flag.String("nats", nats.DefaultURL, "NATS server URL")
	var cfg decode.Config
	flag.StringVar(&cfg.RawStream, "raw-stream", decode.RawStream, "raw stream to read")
	flag.StringVar(&cfg.StateStream, "state-stream", decode.StateStream, "state stream, for session updates and descriptors received before the first frame")
	outStream := flag.String("stream", decode.DefaultStream, "decoded stream to write")
	flag.StringVar(&cfg.Consumer, "consumer", decode.DefaultConsumer, "durable consumer on the raw stream")
	flag.StringVar(&cfg.Start, "start", "new", "where a newly created consumer starts: new, all, or a duration such as 2h")
	flag.IntVar(&cfg.Batch, "batch", 500, "raw messages fetched at a time")
	flag.DurationVar(&cfg.StatsEvery, "stats-interval", time.Minute, "how often to log counters; 0 to disable")
	createStream := flag.Bool("create-stream", true, "create the decoded stream if it does not exist (an existing stream is not changed)")
	maxAge := flag.Duration("max-age", 24*time.Hour, "max age of messages in a newly created decoded stream")
	maxBytes := flag.Int64("max-bytes", 50<<30, "max bytes of a newly created decoded stream, counted before compression; -1 for no limit")
	logJSON := flag.Bool("log-json", false, "log as JSON lines")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if *logJSON {
		log = slog.New(slog.NewJSONHandler(os.Stderr, nil))
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, *natsURL, cfg, *outStream, *createStream, *maxAge, *maxBytes, log); err != nil {
		log.Error("exiting", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, url string, cfg decode.Config, outStream string, create bool, maxAge time.Duration, maxBytes int64, log *slog.Logger) error {
	nc, err := nats.Connect(url, nats.Name("hsdi-decode"), nats.MaxReconnects(-1),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) { log.Warn("NATS disconnected", "err", err) }),
		nats.ReconnectHandler(func(c *nats.Conn) { log.Info("NATS reconnected", "url", c.ConnectedUrl()) }))
	if err != nil {
		return err
	}
	defer nc.Drain()
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	if _, err := js.Stream(ctx, outStream); errors.Is(err, jetstream.ErrStreamNotFound) {
		if !create {
			return fmt.Errorf("stream %s does not exist and -create-stream=false", outStream)
		}
		if _, err := js.CreateStream(ctx, decode.StreamConfig(outStream, maxAge, maxBytes)); err != nil {
			return fmt.Errorf("create stream %s: %w", outStream, err)
		}
		log.Info("created stream", "stream", outStream, "maxAge", maxAge.String())
	} else if err != nil {
		return fmt.Errorf("look up stream %s: %w", outStream, err)
	}
	sink, err := decode.NewNATSSink(nc, cfg.Batch)
	if err != nil {
		return err
	}
	return decode.Run(ctx, nc, cfg, sink, log)
}
