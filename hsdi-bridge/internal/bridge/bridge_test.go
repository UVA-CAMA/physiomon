package bridge

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/UVA-CAMA/physiomon/hsdi-bridge/internal/fakegw"
	"github.com/UVA-CAMA/physiomon/hsdi-bridge/internal/hsdi"
	"github.com/UVA-CAMA/physiomon/hsdi-bridge/internal/natspub"
	"github.com/UVA-CAMA/physiomon/hsdi-bridge/internal/raw"
)

type env struct {
	t   *testing.T
	js  jetstream.JetStream
	gw  *fakegw.Server
	br  *Bridge
	pub *natspub.Publisher
	ctx context.Context
	end context.CancelFunc
	ran chan struct{}
}

func testLog() *slog.Logger {
	if os.Getenv("HSDI_TEST_LOG") != "" {
		return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// start runs a NATS server, a fake gateway and a bridge.
func start(t *testing.T, gopts fakegw.Options, cfg Config) *env {
	t.Helper()
	ns, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats not ready")
	}
	t.Cleanup(ns.Shutdown)
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	pub, err := natspub.New(nc, 1000, testLog())
	if err != nil {
		t.Fatal(err)
	}
	js := pub.JetStream()
	if _, err := js.CreateStream(context.Background(), raw.StreamConfig(raw.DefaultStream, time.Hour, -1)); err != nil {
		t.Fatal(err)
	}
	if _, err := js.CreateStream(context.Background(), raw.StateStreamConfig(raw.DefaultStateStream, -1)); err != nil {
		t.Fatal(err)
	}

	gw, err := fakegw.Start("127.0.0.1:0", gopts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(gw.Close)

	cfg.Name, cfg.Addr = "test", gw.Addr()
	if cfg.BackoffMin == 0 {
		cfg.BackoffMin = 20 * time.Millisecond
	}
	if cfg.ReRequestDelay == 0 {
		cfg.ReRequestDelay = 50 * time.Millisecond
	}
	br := New(cfg, pub, testLog())
	ctx, cancel := context.WithCancel(context.Background())
	e := &env{t: t, js: js, gw: gw, br: br, pub: pub, ctx: ctx, end: cancel, ran: make(chan struct{})}
	go func() { br.Run(ctx); close(e.ran) }()
	t.Cleanup(e.stop)
	return e
}

func (e *env) stop() {
	e.end()
	select {
	case <-e.ran:
	case <-time.After(10 * time.Second):
		e.t.Error("bridge did not stop")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	e.pub.Flush(ctx)
}

// messages reads everything in the raw stream.
func (e *env) messages() []jetstream.Msg {
	e.t.Helper()
	return e.read(raw.DefaultStream)
}

// read reads everything in a stream.
func (e *env) read(stream string) []jetstream.Msg {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	e.pub.Flush(ctx)
	cons, err := e.js.OrderedConsumer(ctx, stream, jetstream.OrderedConsumerConfig{})
	if err != nil {
		e.t.Fatal(err)
	}
	info, err := cons.Info(ctx)
	if err != nil {
		e.t.Fatal(err)
	}
	var out []jetstream.Msg
	want := int(info.NumPending)
	for len(out) < want {
		batch, err := cons.Fetch(want-len(out), jetstream.FetchMaxWait(2*time.Second))
		if err != nil {
			e.t.Fatal(err)
		}
		n := 0
		for m := range batch.Messages() {
			out = append(out, m)
			n++
		}
		if n == 0 {
			break
		}
	}
	return out
}

// count counts messages with direction dir and type typ; "" matches any.
func count(ms []jetstream.Msg, dir, typ string) int {
	n := 0
	for _, m := range ms {
		name, _ := raw.ParseSubject(m.Subject())
		if (dir == "" || name.Dir == dir) && (typ == "" || name.Type == typ) {
			n++
		}
	}
	return n
}

// checkSeq checks that each connection's messages are numbered 1, 2, 3, ...
// in stream order.
func checkSeq(t *testing.T, ms []jetstream.Msg) {
	t.Helper()
	next := map[string]int{}
	for _, m := range ms {
		c := m.Headers().Get(raw.HConn)
		seq, _ := strconv.Atoi(m.Headers().Get(raw.HSeq))
		if next[c] == 0 {
			next[c] = 1
		}
		if seq != next[c] {
			t.Fatalf("conn %s: seq %d, want %d", c, seq, next[c])
		}
		next[c]++
	}
}

func waitUntil(t *testing.T, what string, timeout time.Duration, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !f() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestConversationAndLongHeaders(t *testing.T) {
	e := start(t, fakegw.Options{WaveInterval: 20 * time.Millisecond, NumericInterval: 100 * time.Millisecond, ExtraHeaderLines: 12}, Config{})
	waitUntil(t, "frames", 5*time.Second, func() bool { return e.br.Stats.BinaryParts.Load() >= 40 })
	e.stop()

	gw := e.gw
	for root, want := range map[string]int{"hello": 1, "getSessionUpdateRequest": 1, "getStreamRequest": 2, "endStreamRequest": 1, "endSessionUpdateRequest": 1} {
		if got := gw.Count(root); got != want {
			t.Errorf("gateway received %d %s, want %d", got, root, want)
		}
	}
	// Acks: session update, two binHeaders, four descriptors, at least.
	if got := gw.Count("genericResponse"); got < 7 {
		t.Errorf("gateway received %d acks, want at least 7", got)
	}
	for _, r := range gw.Received() {
		if r.Header.Root == "genericResponse" && r.Header.ReplyToMsgSQN < 1 {
			t.Errorf("ack without replytoMsgSQN: %s", r.Body)
		}
	}

	if e.br.Stats.FramingErrors.Load() != 0 {
		t.Errorf("framing errors with long headers: %d", e.br.Stats.FramingErrors.Load())
	}
	if e.br.Stats.MaxHeaderLines.Load() < 15 {
		t.Errorf("max header lines %d; extra lines not seen", e.br.Stats.MaxHeaderLines.Load())
	}

	ms := e.messages()
	checkSeq(t, ms)
	if n := count(ms, raw.DirIn, raw.TypeBinary); int64(n) != e.br.Stats.BinaryParts.Load() {
		t.Errorf("stream has %d binary messages, bridge read %d", n, e.br.Stats.BinaryParts.Load())
	}
	if count(ms, raw.DirIn, "binDescriptor") != 4 || count(ms, raw.DirIn, "sessionUpdate") != 1 {
		t.Errorf("descriptors %d, session updates %d", count(ms, raw.DirIn, "binDescriptor"), count(ms, raw.DirIn, "sessionUpdate"))
	}
	if count(ms, raw.DirOut, "") != int(e.br.Stats.Sent.Load()) || count(ms, raw.DirOut, "hello") != 1 {
		t.Errorf("sent messages not all in stream")
	}
	if ms[0].Subject() != "hsdi.raw.test.event.connected" || ms[len(ms)-1].Subject() != "hsdi.raw.test.event.disconnected" {
		t.Errorf("stream should start and end with connect and disconnect events")
	}
	var ev raw.Event
	json.Unmarshal(ms[len(ms)-1].Data(), &ev)
	if ev.Reason != "shutdown" {
		t.Errorf("last event %+v", ev)
	}
	// Binary payloads are the frames as sent, and frames carry only the three
	// headers every message has.
	for _, m := range ms {
		if name, _ := raw.ParseSubject(m.Subject()); name.Type != raw.TypeBinary {
			continue
		}
		if m.Data()[0]>>6 != 1 {
			t.Fatalf("binary payload does not start with a format ID")
		}
		h := m.Headers()
		if len(h) != 3 || h.Get(raw.HConn) == "" || h.Get(raw.HSeq) == "" || h.Get(raw.HReceived) == "" {
			t.Fatalf("frame headers %v", h)
		}
	}

	// The state stream has a copy of each descriptor, binHeader and session
	// update, and the events, with the raw message's headers and body.
	rawBySeq := map[string]jetstream.Msg{}
	for _, m := range ms {
		rawBySeq[m.Headers().Get(raw.HSeq)] = m
	}
	state := e.read(raw.DefaultStateStream)
	byType := map[string]int{}
	for _, m := range state {
		r := rawBySeq[m.Headers().Get(raw.HSeq)]
		if r == nil || !bytes.Equal(r.Data(), m.Data()) {
			t.Fatalf("state message %s does not match raw message %s", m.Subject(), m.Headers().Get(raw.HSeq))
		}
		name, _ := raw.ParseSubject(r.Subject())
		byType[name.Type]++
		var want string
		if name.Dir == raw.DirEvent {
			want = raw.StateEventSubject("test", name.Type)
		} else {
			h, _ := hsdi.ParseHeader(r.Data())
			want, _ = raw.StateSubject("test", h.Root, h.SessionID, h.Chn)
			if h.Root == "binDescriptor" && want != "hsdi.state.test.binDescriptor."+h.SessionID+"."+strconv.Itoa(h.Chn) {
				t.Errorf("descriptor state subject %s", want)
			}
		}
		if m.Subject() != want {
			t.Errorf("state subject %s, want %s", m.Subject(), want)
		}
	}
	if byType["binDescriptor"] != 4 || byType["sessionUpdate"] != 1 || byType["binHeader"] < 1 ||
		byType["connected"] != 1 || byType["disconnected"] != 1 || len(state) != int(e.br.Stats.StateMessages.Load()) {
		t.Errorf("state stream has %v, %d messages; bridge published %d", byType, len(state), e.br.Stats.StateMessages.Load())
	}
}

func TestReRequestAfterBinEnd(t *testing.T) {
	e := start(t, fakegw.Options{WaveInterval: 20 * time.Millisecond, BinEndAfter: 10}, Config{})
	if !e.gw.WaitFor("getStreamRequest", 3, 5*time.Second) {
		t.Fatalf("no re-request; stream requests %d", e.gw.Count("getStreamRequest"))
	}
	var body string
	for _, r := range e.gw.Received() {
		if r.Header.Root == "getStreamRequest" {
			body = r.Body
		}
	}
	if !strings.Contains(body, "7E01E110000007D04A3AAC85") || !strings.Contains(body, "/wav/") {
		t.Errorf("re-request is not for the ended session's waveforms: %s", body)
	}
	if e.br.Stats.BinEnds.Load() != 1 || e.br.Stats.ReRequests.Load() != 1 {
		t.Errorf("binEnds %d, reRequests %d", e.br.Stats.BinEnds.Load(), e.br.Stats.ReRequests.Load())
	}
}

func TestReconnectAfterDrop(t *testing.T) {
	e := start(t, fakegw.Options{WaveInterval: 20 * time.Millisecond, DropAfter: 10}, Config{})
	waitUntil(t, "second connection with data", 5*time.Second, func() bool {
		return e.gw.Connections() >= 2 && e.br.Connected() && e.br.Stats.BinaryParts.Load() > 20
	})
	e.stop()
	ms := e.messages()
	checkSeq(t, ms)
	conns := map[string]bool{}
	for _, m := range ms {
		conns[m.Headers().Get(raw.HConn)] = true
	}
	if len(conns) < 2 {
		t.Errorf("messages from %d connections, want 2", len(conns))
	}
	if count(ms, raw.DirEvent, "") < 4 {
		t.Errorf("want connect and disconnect events for both connections")
	}
}

func TestGarbageIsSkipped(t *testing.T) {
	e := start(t, fakegw.Options{WaveInterval: 20 * time.Millisecond, GarbageAfter: 5}, Config{})
	waitUntil(t, "frames after garbage", 5*time.Second, func() bool { return e.br.Stats.BinaryParts.Load() > 30 })
	if e.br.Stats.DiscardedBytes.Load() == 0 {
		t.Error("garbage not counted")
	}
	if e.gw.Connections() != 1 {
		t.Errorf("reconnected %d times; garbage should not drop the connection", e.gw.Connections()-1)
	}
	e.stop()
	ms := e.messages()
	if count(ms, raw.DirIn, raw.TypeDiscarded) != 1 {
		t.Fatalf("%d discarded messages, want 1", count(ms, raw.DirIn, raw.TypeDiscarded))
	}
	for _, m := range ms {
		if m.Subject() == "hsdi.raw.test.in.discarded" {
			if n, _ := strconv.Atoi(m.Headers().Get(raw.HDiscarded)); int64(n) != e.br.Stats.DiscardedBytes.Load() || !strings.Contains(string(m.Data()), "not a MIME message") {
				t.Errorf("discarded message: %s bytes, %q", m.Headers().Get(raw.HDiscarded), m.Data())
			}
		}
	}
}

func TestKeepAliveDetectsDeadGateway(t *testing.T) {
	e := start(t, fakegw.Options{Silent: true, IgnoreKeepAlive: true},
		Config{KeepAliveIdle: 300 * time.Millisecond, KeepAliveTimeout: 1500 * time.Millisecond})
	waitUntil(t, "reconnect", 10*time.Second, func() bool { return e.gw.Connections() >= 2 })
	if e.gw.Count("keepAliveRequest") < 1 {
		t.Error("no keep-alive sent")
	}
}

// A real gateway accepted connections but ignored hello for an hour.
func TestHelloFailuresCounted(t *testing.T) {
	e := start(t, fakegw.Options{IgnoreHello: true}, Config{HelloTimeout: 100 * time.Millisecond, BackoffMax: 20 * time.Millisecond})
	waitUntil(t, "failed attempts", 10*time.Second, func() bool {
		n, _ := e.br.Down()
		return n >= failuresBeforeError+1
	})
	if e.br.Connected() {
		t.Error("connected without a hello reply")
	}
}

func TestHelloFailuresReset(t *testing.T) {
	e := start(t, fakegw.Options{DropAfter: 5}, Config{})
	waitUntil(t, "reconnect", 10*time.Second, func() bool { return e.gw.Connections() >= 2 && e.br.Connected() })
	if n, _ := e.br.Down(); n != 0 {
		t.Errorf("failed attempts %d after a working connection", n)
	}
}

func TestKeepAliveKeepsQuietGateway(t *testing.T) {
	e := start(t, fakegw.Options{Silent: true},
		Config{KeepAliveIdle: 300 * time.Millisecond, KeepAliveTimeout: 1500 * time.Millisecond})
	time.Sleep(4 * time.Second)
	if e.gw.Connections() != 1 {
		t.Errorf("quiet but answering gateway was dropped")
	}
	if e.gw.Count("keepAliveRequest") < 2 {
		t.Errorf("keep-alives sent: %d", e.gw.Count("keepAliveRequest"))
	}
}

// A real gateway refused every re-request after "removed from data
// acquisition"; the bridge no longer sends them.
func TestNoReRequestWhenRemoved(t *testing.T) {
	e := start(t, fakegw.Options{WaveInterval: 20 * time.Millisecond, BinEndAfter: 10,
		BinEndMessage: "BinEnd sent for sessionID 7E01E110000007D04A3AAC85 to client 7EC46464000007D74A3AAC99 as the session was removed from data acquisition."}, Config{})
	waitUntil(t, "binEnd", 5*time.Second, func() bool { return e.br.Stats.BinEnds.Load() == 1 })
	time.Sleep(4 * e.br.cfg.ReRequestDelay)
	if n := e.gw.Count("getStreamRequest"); n != 2 {
		t.Errorf("stream requests %d, want the initial 2 only", n)
	}
	if e.br.Stats.BinEndsNotReRequested.Load() != 1 || e.br.Stats.ReRequests.Load() != 0 {
		t.Errorf("not re-requested %d, re-requests %d", e.br.Stats.BinEndsNotReRequested.Load(), e.br.Stats.ReRequests.Load())
	}
}

// End requests at shutdown name only the requests the gateway accepted, and
// closing the socket ourselves is not a framing error.
func TestShutdownNamesAcceptedRequests(t *testing.T) {
	e := start(t, fakegw.Options{WaveInterval: 20 * time.Millisecond, BinEndAfter: 10, NakSessionRequests: true}, Config{})
	waitUntil(t, "refused re-request", 5*time.Second, func() bool { return e.br.Stats.Naks.Load() == 1 })
	e.stop()

	var ids [][]string
	for _, r := range e.gw.Received() {
		switch r.Header.Root {
		case "getStreamRequest":
			ids = append(ids, []string{strconv.FormatInt(r.Header.RequestID, 10)})
		case "endStreamRequest":
			if strings.Count(r.Body, "<originalRequestID") != 2 {
				t.Errorf("end request names %d requests, want 2:\n%s", strings.Count(r.Body, "<originalRequestID"), r.Body)
			}
			refused := ids[len(ids)-1][0]
			if strings.Contains(r.Body, `<originalRequestID V="`+refused+`"`) {
				t.Errorf("end request names refused request %s:\n%s", refused, r.Body)
			}
		}
	}
	if len(ids) != 3 {
		t.Fatalf("stream requests %d, want 3", len(ids))
	}
	if n := e.br.Stats.FramingErrors.Load(); n != 0 {
		t.Errorf("framing errors %d at shutdown", n)
	}
	if n := count(e.messages(), raw.DirEvent, raw.EventFramingError); n != 0 {
		t.Errorf("%d framing-error events", n)
	}
}

// A real gateway sends keep-alive requests as bare XML. The bridge reads them
// as messages, publishes them and answers each one.
func TestBareKeepAliveAnswered(t *testing.T) {
	e := start(t, fakegw.Options{BareKeepAliveEvery: 50 * time.Millisecond, Silent: true}, Config{})
	if !e.gw.WaitFor("keepAliveResponse", 2, 5*time.Second) {
		t.Fatalf("keep-alive responses %d", e.gw.Count("keepAliveResponse"))
	}
	e.stop()
	for _, r := range e.gw.Received() {
		if r.Header.Root == "keepAliveResponse" && (r.Header.ResponseID < 15001 || r.Header.ReplyToMsgSQN < 1 || r.Header.Status != "ack") {
			t.Errorf("response %+v", r.Header)
		}
	}
	ms := e.messages()
	if n := count(ms, raw.DirIn, "keepAliveRequest"); n < 2 || int64(n) != e.br.Stats.BareMessages.Load() {
		t.Errorf("%d keep-alive requests in the stream, %d bare messages counted", n, e.br.Stats.BareMessages.Load())
	}
	if n := count(ms, raw.DirIn, raw.TypeDiscarded); n != 0 || e.br.Stats.DiscardedBytes.Load() != 0 {
		t.Errorf("%d discarded messages", n)
	}
}
