package mllp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultAddr is what Listen listens on when given no addresses.
const DefaultAddr = ":2575"

// ListenFlag is a repeatable --listen flag. Each value is name=addr, such as
// WCM=:2577, or just addr, in which case the name is the address.
type ListenFlag []string

func (l *ListenFlag) String() string     { return strings.Join(*l, ",") }
func (l *ListenFlag) Set(v string) error { *l = append(*l, v); return nil }

// Feed is one named listener. The name labels its log lines.
type Feed struct {
	Name     string
	Listener net.Listener
}

// Listen opens a listener for each name=addr in specs, or for DefaultAddr if
// specs is empty. If any fails, it closes the ones it opened.
func Listen(specs []string) ([]Feed, error) {
	if len(specs) == 0 {
		specs = []string{DefaultAddr}
	}
	var feeds []Feed
	fail := func(err error) ([]Feed, error) {
		for _, f := range feeds {
			f.Listener.Close()
		}
		return nil, err
	}
	seen := map[string]bool{}
	for _, s := range specs {
		name, addr, ok := strings.Cut(s, "=")
		if !ok {
			name, addr = s, s
		}
		if name == "" || addr == "" {
			return fail(fmt.Errorf("bad --listen %q: want name=addr, such as WCM=:2577", s))
		}
		if seen[name] {
			return fail(fmt.Errorf("--listen name %q given twice", name))
		}
		seen[name] = true
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			return fail(fmt.Errorf("listen %s on %s: %w", name, addr, err))
		}
		feeds = append(feeds, Feed{Name: name, Listener: ln})
	}
	return feeds, nil
}

type counters struct {
	open, messages, bytes, failed, bytesRead atomic.Int64
}

// Serve accepts connections on every feed until ctx is done, then closes the
// listeners and returns. Each connection gets its own publish function from
// newPublish, and its log lines carry "feed". Every statsEvery (0 to
// disable), Serve logs each feed's counters: open connections, and since it
// started, messages, their bytes, failed publishes, and all bytes received.
// Connections still open at return are left to the caller, which exits.
func Serve(ctx context.Context, feeds []Feed, newPublish func() func(msg []byte) error,
	sendACK bool, statsEvery time.Duration, logger *slog.Logger) {
	stats := make([]*counters, len(feeds))
	var wg sync.WaitGroup
	for i, f := range feeds {
		stats[i] = &counters{}
		log := logger.With("feed", f.Name)
		log.Info("listening", "addr", f.Listener.Addr().String())
		wg.Add(1)
		go func(f Feed, c *counters) {
			defer wg.Done()
			for {
				conn, err := f.Listener.Accept()
				if err != nil {
					if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
						return
					}
					log.Error("accept error", "err", err)
					continue
				}
				go handle(conn, newPublish(), sendACK, log, c)
			}
		}(f, stats[i])
	}

	var tick <-chan time.Time
	if statsEvery > 0 {
		t := time.NewTicker(statsEvery)
		defer t.Stop()
		tick = t.C
	}
	for done := false; !done; {
		select {
		case <-ctx.Done():
			done = true
		case <-tick:
			for i, f := range feeds {
				c := stats[i]
				logger.Info("counters", "feed", f.Name, "connections", c.open.Load(),
					"messages", c.messages.Load(), "bytes", c.bytes.Load(), "publishFailed", c.failed.Load(),
					"bytesRead", c.bytesRead.Load())
			}
		}
	}

	logger.Info("shutdown signal received, closing listeners")
	for _, f := range feeds {
		f.Listener.Close()
	}
	wg.Wait()
}
