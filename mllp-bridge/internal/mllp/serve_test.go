package mllp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// syncBuffer is a log destination that connections may still write to
// after Serve returns.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestListenNamesFeeds(t *testing.T) {
	feeds, err := Listen([]string{"WCM=127.0.0.1:0", "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		for _, f := range feeds {
			f.Listener.Close()
		}
	}()
	if feeds[0].Name != "WCM" || feeds[1].Name != "127.0.0.1:0" {
		t.Errorf("names %q, %q; want WCM and the address", feeds[0].Name, feeds[1].Name)
	}
}

func TestListenRejectsBadSpecs(t *testing.T) {
	for _, specs := range [][]string{
		{"WCM="},
		{"=:2577"},
		{"WCM=127.0.0.1:0", "WCM=127.0.0.1:0"},
	} {
		if feeds, err := Listen(specs); err == nil {
			for _, f := range feeds {
				f.Listener.Close()
			}
			t.Errorf("Listen(%q) succeeded, want an error", specs)
		}
	}
}

func TestListenClosesOpenedListenersOnFailure(t *testing.T) {
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer taken.Close()
	free, _ := net.Listen("tcp", "127.0.0.1:0")
	freeAddr := free.Addr().String()
	free.Close()

	if _, err := Listen([]string{"A=" + freeAddr, "B=" + taken.Addr().String()}); err == nil {
		t.Fatal("Listen on a taken port succeeded")
	}
	// A was opened and must have been closed again.
	ln, err := net.Listen("tcp", freeAddr)
	if err != nil {
		t.Fatalf("A's port is still held: %v", err)
	}
	ln.Close()
}

func TestServeFeeds(t *testing.T) {
	feeds, err := Listen([]string{"DEC=127.0.0.1:0", "WCM=127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan string, 4)
	var logs syncBuffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		Serve(ctx, feeds, func() func([]byte) error {
			return func(msg []byte) error { got <- string(msg); return nil }
		}, false, 20*time.Millisecond, logger)
		close(done)
	}()

	for _, f := range feeds {
		conn, err := net.Dial("tcp", f.Listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		Write(conn, []byte("MSH|"+f.Name))
		select {
		case m := <-got:
			if m != "MSH|"+f.Name {
				t.Errorf("published %q, want MSH|%s", m, f.Name)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("nothing published from %s", f.Name)
		}
	}
	time.Sleep(100 * time.Millisecond) // let a counters line be logged
	cancel()
	<-done

	out := logs.String()
	for _, want := range []string{
		`msg="connection accepted" feed=DEC`,
		`msg="connection accepted" feed=WCM`,
		`msg=counters feed=WCM connections=1 messages=1 bytes=7 publishFailed=0 bytesRead=10`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("logs lack %s:\n%s", want, out)
		}
	}
	for _, f := range feeds {
		if c, err := net.Dial("tcp", f.Listener.Addr().String()); err == nil {
			c.Close()
			t.Errorf("%s still accepts connections after Serve returned", f.Name)
		}
	}
}

func TestPublishFailureClosesConnection(t *testing.T) {
	feeds, err := Listen([]string{"DEC=127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	go Serve(ctx, feeds, func() func([]byte) error {
		return func([]byte) error { return errors.New("broker down") }
	}, false, 0, logger)

	conn, err := net.Dial("tcp", feeds[0].Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	Write(conn, []byte("MSH|x"))
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Read(make([]byte, 1)); err != io.EOF {
		t.Errorf("read after a failed publish: %v, want EOF (connection closed)", err)
	}
}

func TestUnframedBytesAreCounted(t *testing.T) {
	feeds, err := Listen([]string{"DEC=127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	var logs syncBuffer
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		Serve(ctx, feeds, func() func([]byte) error { return func([]byte) error { return nil } },
			false, 20*time.Millisecond, slog.New(slog.NewTextHandler(&logs, nil)))
		close(done)
	}()
	conn, err := net.Dial("tcp", feeds[0].Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.Write([]byte("MSH|no framing\r")) // 15 bytes, no start block
	time.Sleep(100 * time.Millisecond)
	cancel()
	<-done
	if want := "messages=0 bytes=0 publishFailed=0 bytesRead=15"; !strings.Contains(logs.String(), want) {
		t.Errorf("logs lack %s:\n%s", want, logs.String())
	}
}
