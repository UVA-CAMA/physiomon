package decode

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// NATSSink publishes frames to the decoded stream, encoded as
// proto/decoded.proto describes, on Subject(gateway, service). Each message's
// Nats-Msg-Id is the frame's raw sequence number.
type NATSSink struct {
	js      jetstream.JetStream
	pending []jetstream.PubAckFuture
}

// NewNATSSink returns a NATSSink on nc that holds at most maxPending
// unacknowledged messages; the run loop flushes after each batch, so
// maxPending must be at least the batch size.
func NewNATSSink(nc *nats.Conn, maxPending int) (*NATSSink, error) {
	js, err := jetstream.New(nc, jetstream.WithPublishAsyncMaxPending(maxPending))
	if err != nil {
		return nil, err
	}
	return &NATSSink{js: js}, nil
}

func (s *NATSSink) Write(f *Frame) error {
	m := nats.NewMsg(Subject(f.Gateway, f.Service))
	m.Data = Encode(f)
	m.Header.Set(jetstream.MsgIDHeader, strconv.FormatUint(f.RawSeq, 10))
	fut, err := s.js.PublishMsgAsync(m)
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	s.pending = append(s.pending, fut)
	return nil
}

func (s *NATSSink) Flush(ctx context.Context) error {
	defer func() { s.pending = s.pending[:0] }()
	timeout := time.After(time.Minute)
	for _, f := range s.pending {
		select {
		case <-f.Ok():
		case err := <-f.Err():
			return fmt.Errorf("publish: %w", err)
		case <-timeout:
			return errors.New("publish: no acknowledgement in a minute")
		}
	}
	return nil
}
