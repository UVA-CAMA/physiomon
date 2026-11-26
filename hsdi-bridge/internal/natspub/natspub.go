// Package natspub publishes to JetStream asynchronously, in call order, with
// a bound on unacknowledged messages.
//
// When the bound is reached, Publish waits. That holds up the bridge's read
// loop and so, through TCP, the gateway. A gateway held up long enough ends
// streams with "Client too slow" (manual 3-26), and the bridge requests them
// again, so a stall shows in the counters rather than as silent loss.
package natspub

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/UVA-CAMA/physiomon/hsdi-bridge/internal/raw"
)

// Publisher implements bridge.Publisher.
type Publisher struct {
	js  jetstream.JetStream
	log *slog.Logger

	Stalls    atomic.Int64 // waits because too many messages were unacknowledged
	Retries   atomic.Int64 // other publish errors that were retried
	AckErrors atomic.Int64 // messages the server did not acknowledge
}

// New returns a Publisher on nc with at most maxPending unacknowledged
// messages.
func New(nc *nats.Conn, maxPending int, log *slog.Logger) (*Publisher, error) {
	p := &Publisher{log: log}
	js, err := jetstream.New(nc,
		jetstream.WithPublishAsyncMaxPending(maxPending),
		jetstream.WithPublishAsyncTimeout(30*time.Second),
		jetstream.WithPublishAsyncErrHandler(func(_ jetstream.JetStream, m *nats.Msg, err error) {
			p.AckErrors.Add(1)
			log.Error("message not acknowledged by JetStream", "subject", m.Subject, "conn", m.Header.Get(raw.HConn), "seq", m.Header.Get(raw.HSeq), "err", err)
		}))
	if err != nil {
		return nil, err
	}
	p.js = js
	return p, nil
}

// JetStream returns the underlying JetStream context.
func (p *Publisher) JetStream() jetstream.JetStream { return p.js }

// Publish sends m. It retries while the pending bound is reached, and retries
// other errors for up to about ten seconds before giving up.
func (p *Publisher) Publish(ctx context.Context, m *nats.Msg) error {
	var last error
	for attempt := 0; attempt < 100; {
		_, err := p.js.PublishMsgAsync(m, jetstream.WithStallWait(time.Second))
		if err == nil {
			return nil
		}
		if errors.Is(err, jetstream.ErrTooManyStalledMsgs) {
			p.Stalls.Add(1)
		} else {
			attempt++
			p.Retries.Add(1)
			last = err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return last
}

// Flush waits until all published messages are acknowledged or ctx ends.
func (p *Publisher) Flush(ctx context.Context) error {
	select {
	case <-p.js.PublishAsyncComplete():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
