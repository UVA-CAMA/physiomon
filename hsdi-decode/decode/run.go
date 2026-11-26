package decode

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	// DefaultStream and DefaultConsumer are the names hsdi-decode uses
	// unless told otherwise.
	DefaultStream   = "HSDI_DECODED"
	DefaultConsumer = "hsdi-decode"
	// SubjectPrefix starts every decoded subject:
	// hsdi.decoded.<gateway>.<wav|num>.
	SubjectPrefix = "hsdi.decoded"
)

// Subject returns the subject for a decoded frame.
func Subject(gateway, service string) string {
	return SubjectPrefix + "." + gateway + "." + service
}

// StreamConfig returns the configuration hsdi-decode uses when it creates the
// decoded stream. Duplicates lets JetStream drop the copies a restart
// publishes again, by raw sequence number, if the restart comes within it.
func StreamConfig(name string, maxAge time.Duration, maxBytes int64) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:        name,
		Description: "Decoded frames from HSDI gateways, written by hsdi-decode",
		Subjects:    []string{SubjectPrefix + ".>"},
		Storage:     jetstream.FileStorage,
		Compression: jetstream.S2Compression,
		Retention:   jetstream.LimitsPolicy,
		Discard:     jetstream.DiscardOld,
		MaxAge:      maxAge,
		MaxBytes:    maxBytes,
		Duplicates:  2 * time.Minute,
	}
}

// Config is what Run needs.
type Config struct {
	RawStream, StateStream string
	Consumer               string // durable consumer on the raw stream
	// Start applies only when the consumer is created: "new" for messages
	// published from then on, "all" for the whole raw stream, or a duration
	// such as "2h" for messages received in that period before then.
	Start      string
	Batch      int
	StatsEvery time.Duration
}

// rawFilters are the raw subjects the decoder reads.
var rawFilters = []string{
	rawSubject("*", typeBinary),
	rawSubject("*", "sessionUpdate"),
	rawSubject("*", "binDescriptor"),
}

// Sink receives decoded frames. Write may hold on to frames; Flush returns
// when every frame written so far is stored.
type Sink interface {
	Write(f *Frame) error
	Flush(ctx context.Context) error
}

// Run decodes the raw stream into sink until ctx ends.
//
// Raw messages are acknowledged only after sink has stored their frames, so
// after a crash the decoder starts again from the first frame not stored.
// Before its first raw message it reads, from the state stream, the session
// updates and descriptors received up to that message, so frames get the
// patient and descriptor that applied when they arrived.
func Run(ctx context.Context, nc *nats.Conn, cfg Config, sink Sink, log *slog.Logger) error {
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	cons, err := consumer(ctx, js, cfg, log)
	if err != nil {
		return err
	}
	dec := New()
	started := false
	var written, errs int64
	lastStats := time.Now()
	for ctx.Err() == nil {
		batch, err := cons.Fetch(cfg.Batch, jetstream.FetchMaxWait(2*time.Second))
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			return fmt.Errorf("fetch: %w", err)
		}
		var last jetstream.Msg
		for m := range batch.Messages() {
			if !started {
				t := received(m.Headers())
				if err := preload(ctx, js, cfg.StateStream, t, dec, log); err != nil {
					return err
				}
				started = true
			}
			md, err := m.Metadata()
			if err != nil {
				return err
			}
			f, err := apply(dec, m, md.Sequence.Stream)
			if err != nil {
				errs++
				if errs <= 10 {
					log.Warn("message not decoded", "subject", m.Subject(), "rawSeq", md.Sequence.Stream, "err", err)
				}
			}
			if f != nil {
				if err := sink.Write(f); err != nil {
					return err
				}
				written++
			}
			last = m
		}
		if err := batch.Error(); err != nil && !errors.Is(err, nats.ErrTimeout) && ctx.Err() == nil {
			return fmt.Errorf("fetch: %w", err)
		}
		if last != nil {
			if err := sink.Flush(ctx); err != nil {
				return err
			}
			if err := last.Ack(); err != nil {
				return fmt.Errorf("ack: %w", err)
			}
		}
		if cfg.StatsEvery > 0 && time.Since(lastStats) >= cfg.StatsEvery {
			lastStats = time.Now()
			attrs := []any{"written", written, "notDecoded", errs, "stats", dec.Stats}
			if info, err := cons.Info(ctx); err == nil {
				attrs = append(attrs, "rawPending", info.NumPending)
			}
			log.Info("counters", attrs...)
		}
	}
	return nil
}

// apply passes one raw message to dec, and returns the decoded frame, if any.
func apply(dec *Decoder, m jetstream.Msg, seq uint64) (*Frame, error) {
	gw, typ, ok := parseSubject(m.Subject())
	if !ok {
		return nil, errors.New("unexpected subject")
	}
	switch typ {
	case "sessionUpdate":
		return nil, dec.SessionUpdate(gw, m.Data())
	case "binDescriptor":
		return nil, dec.Descriptor(gw, m.Data())
	case typeBinary:
		return dec.Frame(gw, m.Data(), received(m.Headers()), seq)
	}
	return nil, nil
}

func consumer(ctx context.Context, js jetstream.JetStream, cfg Config, log *slog.Logger) (jetstream.Consumer, error) {
	s, err := js.Stream(ctx, cfg.RawStream)
	if err != nil {
		return nil, fmt.Errorf("raw stream %s: %w", cfg.RawStream, err)
	}
	c, err := s.Consumer(ctx, cfg.Consumer)
	if err == nil {
		log.Info("using existing consumer", "consumer", cfg.Consumer, "pending", c.CachedInfo().NumPending)
		return c, nil
	}
	if !errors.Is(err, jetstream.ErrConsumerNotFound) {
		return nil, err
	}
	cc := jetstream.ConsumerConfig{
		Durable:        cfg.Consumer,
		Description:    "hsdi-decode",
		FilterSubjects: rawFilters,
		AckPolicy:      jetstream.AckAllPolicy,
		AckWait:        time.Minute,
		MaxAckPending:  max(10*cfg.Batch, 10000),
	}
	switch cfg.Start {
	case "new":
		cc.DeliverPolicy = jetstream.DeliverNewPolicy
	case "all":
		cc.DeliverPolicy = jetstream.DeliverAllPolicy
	default:
		d, err := time.ParseDuration(cfg.Start)
		if err != nil {
			return nil, fmt.Errorf("start %q: want new, all or a duration", cfg.Start)
		}
		t := time.Now().Add(-d)
		cc.DeliverPolicy, cc.OptStartTime = jetstream.DeliverByStartTimePolicy, &t
	}
	c, err = s.CreateConsumer(ctx, cc)
	if err != nil {
		return nil, fmt.Errorf("create consumer %s: %w", cfg.Consumer, err)
	}
	log.Info("created consumer", "consumer", cfg.Consumer, "start", cfg.Start)
	return c, nil
}

// preload applies the session updates and descriptors in the state stream
// received at or before t.
func preload(ctx context.Context, js jetstream.JetStream, stream string, t time.Time, dec *Decoder, log *slog.Logger) error {
	s, err := js.Stream(ctx, stream)
	if err != nil {
		return fmt.Errorf("state stream %s: %w", stream, err)
	}
	cons, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{FilterSubjects: []string{
		statePrefix + ".*.sessionUpdate", statePrefix + ".*.binDescriptor.>"}})
	if err != nil {
		return err
	}
	n := 0
	start := time.Now()
	for {
		batch, err := cons.Fetch(500, jetstream.FetchMaxWait(2*time.Second))
		if err != nil {
			return err
		}
		got, done := 0, false
		for m := range batch.Messages() {
			got++
			if received(m.Headers()).After(t) {
				done = true
				break
			}
			n++
			switch gw, typ, _ := parseSubject(m.Subject()); typ {
			case "sessionUpdate":
				dec.SessionUpdate(gw, m.Data())
			case "binDescriptor":
				dec.Descriptor(gw, m.Data())
			}
			if md, err := m.Metadata(); err == nil && md.NumPending == 0 {
				done = true
			}
		}
		if err := batch.Error(); err != nil && !errors.Is(err, nats.ErrTimeout) {
			return err
		}
		// The state stream may be empty; wait a little for the first message.
		if done || (got == 0 && (n > 0 || time.Since(start) > 10*time.Second)) {
			break
		}
	}
	log.Info("read state stream", "messages", n, "upTo", "first raw message", "stats", dec.Stats)
	return nil
}
