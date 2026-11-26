// Package bridge holds the connection to one HSDI gateway and publishes
// what it receives to the raw stream.
//
// It runs the conversation a client must hold (manual 3-6, 3-7): hello,
// subscriptions to session updates and to numeric and waveform streams for
// all sessions, acknowledgements, keep-alive replies, a new stream request
// after a <binEnd>, and a graceful end on shutdown. It reconnects with
// backoff when the connection fails. It never looks inside session updates,
// descriptors or binary frames.
package bridge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/UVA-CAMA/physiomon/hsdi-bridge/internal/hsdi"
	"github.com/UVA-CAMA/physiomon/hsdi-bridge/internal/mime"
	"github.com/UVA-CAMA/physiomon/hsdi-bridge/internal/raw"
)

// Config is the configuration for one gateway.
type Config struct {
	Name string // used in the subject; letters, digits, '-' and '_'
	Addr string // host:port

	DialTimeout  time.Duration
	HelloTimeout time.Duration
	// ResponseTimeout is how long a request may wait for its response before
	// it is logged as late. The manual says two seconds (3-30).
	ResponseTimeout time.Duration
	// KeepAliveIdle: after this long with nothing read, send a keep-alive.
	// KeepAliveTimeout: if it gets no reply in this time, reconnect. The
	// gateway v1 never sends keep-alives itself (3-27), so a dead connection
	// is otherwise found only by TCP, which can take many minutes.
	KeepAliveIdle    time.Duration
	KeepAliveTimeout time.Duration
	// ReRequestDelay is the wait after a <binEnd> before requesting that
	// session's stream again (3-25). Negative disables re-requests.
	ReRequestDelay  time.Duration
	BackoffMin      time.Duration
	BackoffMax      time.Duration
	WriteTimeout    time.Duration
	ShutdownTimeout time.Duration
}

// Defaults fills unset durations.
func (c *Config) Defaults() {
	set := func(d *time.Duration, v time.Duration) {
		if *d == 0 {
			*d = v
		}
	}
	set(&c.DialTimeout, 10*time.Second)
	set(&c.HelloTimeout, 10*time.Second)
	set(&c.ResponseTimeout, 2*time.Second)
	set(&c.KeepAliveIdle, 30*time.Second)
	set(&c.KeepAliveTimeout, 10*time.Second)
	set(&c.ReRequestDelay, 5*time.Second)
	set(&c.BackoffMin, time.Second)
	set(&c.BackoffMax, time.Minute)
	set(&c.WriteTimeout, 10*time.Second)
	set(&c.ShutdownTimeout, 3*time.Second)
}

// Publisher publishes one message to the raw stream, in call order.
type Publisher interface {
	Publish(ctx context.Context, m *nats.Msg) error
}

// Bridge runs one gateway connection, reconnecting until its context ends.
type Bridge struct {
	cfg   Config
	pub   Publisher
	log   *slog.Logger
	Stats Stats

	connCount atomic.Int64
	connected atomic.Bool
	lastRead  atomic.Int64 // Unix nanoseconds
	// failures counts connection attempts in a row that did not complete
	// hello. downSince is when the last completed connection ended, or when
	// the bridge started.
	failures  atomic.Int64
	downSince atomic.Int64 // Unix nanoseconds
}

// New returns a Bridge. cfg.Defaults is applied.
func New(cfg Config, pub Publisher, log *slog.Logger) *Bridge {
	cfg.Defaults()
	b := &Bridge{cfg: cfg, pub: pub, log: log.With("gateway", cfg.Name)}
	b.downSince.Store(time.Now().UnixNano())
	return b
}

// Connected reports whether the bridge has a connection that completed hello.
func (b *Bridge) Connected() bool { return b.connected.Load() }

// Down returns the number of connection attempts in a row that did not
// complete hello, and since when the gateway has had no working connection.
// Both are meaningful only while Connected is false.
func (b *Bridge) Down() (failures int64, since time.Time) {
	return b.failures.Load(), time.Unix(0, b.downSince.Load())
}

// failuresBeforeError is how many attempts in a row may fail before each
// further failure is logged as an error rather than a warning.
const failuresBeforeError = 5

// LastRead returns when the bridge last read from the gateway.
func (b *Bridge) LastRead() time.Time { return time.Unix(0, b.lastRead.Load()) }

// Run connects and reconnects until ctx ends.
func (b *Bridge) Run(ctx context.Context) {
	backoff := b.cfg.BackoffMin
	for ctx.Err() == nil {
		start := time.Now()
		up, err := b.runConn(ctx)
		if ctx.Err() != nil {
			return
		}
		if up {
			b.failures.Store(0)
			b.downSince.Store(time.Now().UnixNano())
		}
		b.log.Warn("connection ended", "err", err, "lasted", time.Since(start).Round(time.Second).String())
		if n, since := b.Down(); n >= failuresBeforeError {
			b.log.Error("gateway not answering", "failedAttempts", n,
				"downFor", time.Since(since).Round(time.Second).String(), "err", err)
		}
		if time.Since(start) > 2*time.Minute {
			backoff = b.cfg.BackoffMin
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, b.cfg.BackoffMax)
	}
}

type pending struct {
	root      string
	requestID int64 // 0 if the request has none
	sentAt    time.Time
	late      bool
}

// conn is the state of one TCP connection.
type conn struct {
	b         *Bridge
	nc        net.Conn
	id        string
	clientSID string

	pubMu sync.Mutex // orders publishing and assigns seq
	seq   uint64

	writeMu sync.Mutex
	msgSQN  int64
	reqID   int64

	mu           sync.Mutex
	pending      map[int64]*pending // by msgSQN of the request
	reRequests   map[string]*time.Timer
	shuttingDown bool
	closed       bool
	// Request IDs the gateway accepted, to name in end requests at shutdown.
	streamReqIDs []int64
	sessReqIDs   []int64

	helloCh chan hsdi.Header
}

// runConn runs one connection. up reports whether it completed hello.
func (b *Bridge) runConn(ctx context.Context) (up bool, err error) {
	defer func() {
		if !up {
			b.failures.Add(1)
		}
	}()
	d := net.Dialer{Timeout: b.cfg.DialTimeout, KeepAlive: 15 * time.Second}
	nc, err := d.DialContext(ctx, "tcp", b.cfg.Addr)
	if err != nil {
		return false, fmt.Errorf("dial: %w", err)
	}
	now := time.Now()
	c := &conn{
		b:          b,
		nc:         nc,
		id:         fmt.Sprintf("%s-%d", now.UTC().Format("20060102T150405Z"), b.connCount.Add(1)),
		pending:    map[int64]*pending{},
		reRequests: map[string]*time.Timer{},
		helloCh:    make(chan hsdi.Header, 1),
	}
	if ta, ok := nc.RemoteAddr().(*net.TCPAddr); ok {
		c.clientSID = hsdi.ClientSessionID(ta, now)
	} else {
		c.clientSID = hsdi.ClientSessionID(&net.TCPAddr{}, now)
	}
	b.Stats.Connects.Add(1)
	b.lastRead.Store(now.UnixNano())
	log := b.log.With("conn", c.id)
	log.Info("connected", "remote", nc.RemoteAddr().String())
	// Publishing uses its own context so that messages read during shutdown
	// are still published.
	pctx := context.WithoutCancel(ctx)
	c.publishEvent(pctx, raw.EventConnected, raw.Event{Remote: nc.RemoteAddr().String()})

	readErr := make(chan error, 1)
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		readErr <- c.readLoop(pctx)
	}()

	reason := c.converse(ctx, readErr)
	up = b.connected.Load()

	c.mu.Lock()
	c.closed = true
	for _, t := range c.reRequests {
		t.Stop()
	}
	c.mu.Unlock()
	nc.Close()
	<-readDone
	b.connected.Store(false)
	b.Stats.Disconnects.Add(1)
	why := "shutdown"
	if reason != nil {
		why = reason.Error()
	}
	c.publishEvent(pctx, raw.EventDisconnected, raw.Event{Reason: why})
	log.Info("disconnected", "reason", why)
	return up, reason
}

// converse runs the conversation until the connection fails or ctx ends. It
// returns nil after a graceful shutdown.
func (c *conn) converse(ctx context.Context, readErr <-chan error) error {
	b := c.b
	bytesBefore := b.Stats.BytesIn.Load()
	if err := c.request("hello", hsdi.Hello, false); err != nil {
		return err
	}
	select {
	case h := <-c.helloCh:
		if h.IsNak() {
			return fmt.Errorf("hello refused: %s %s", h.Status, hsdi.Redact(h.ErrorString))
		}
	case err := <-readErr:
		return fmt.Errorf("before hello response: %w", err)
	case <-time.After(b.cfg.HelloTimeout):
		return fmt.Errorf("no hello response in %s (%d bytes received from the gateway)", b.cfg.HelloTimeout, b.Stats.BytesIn.Load()-bytesBefore)
	case <-ctx.Done():
		return nil
	}

	if err := c.subscribe(); err != nil {
		return err
	}
	b.connected.Store(true)

	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	var keepAliveSent time.Time
	for {
		select {
		case err := <-readErr:
			return err
		case <-ctx.Done():
			c.shutdown(readErr)
			return nil
		case now := <-tick.C:
			c.checkLate(now)
			idle := now.Sub(b.LastRead())
			switch {
			case !keepAliveSent.IsZero() && b.LastRead().After(keepAliveSent):
				keepAliveSent = time.Time{}
			case !keepAliveSent.IsZero() && now.Sub(keepAliveSent) > b.cfg.KeepAliveTimeout:
				return fmt.Errorf("no reply to keep-alive in %s", b.cfg.KeepAliveTimeout)
			case keepAliveSent.IsZero() && idle > b.cfg.KeepAliveIdle:
				if err := c.request("keepAliveRequest", hsdi.KeepAliveRequest, false); err != nil {
					return err
				}
				keepAliveSent = now
			}
		}
	}
}

// subscribe requests session updates and both data streams for all sessions.
func (c *conn) subscribe() error {
	if err := c.request("getSessionUpdateRequest", hsdi.GetSessionUpdateRequest, true); err != nil {
		return err
	}
	for _, sel := range []string{hsdi.SelectNumeric, hsdi.SelectWaveform} {
		if err := c.request("getStreamRequest", func(h hsdi.RequestHeader) []byte {
			return hsdi.GetStreamRequest(h, []string{"*"}, sel)
		}, true); err != nil {
			return err
		}
	}
	return nil
}

// shutdown ends streams and session updates and waits briefly for the
// responses (3-28).
func (c *conn) shutdown(readErr <-chan error) {
	// Name the original request IDs; the manual also allows "*" (3-17,
	// 3-21).
	c.mu.Lock()
	c.shuttingDown = true
	streams, sessions := ids(c.streamReqIDs), ids(c.sessReqIDs)
	c.mu.Unlock()
	if len(streams) > 0 {
		c.request("endStreamRequest", func(h hsdi.RequestHeader) []byte { return hsdi.EndStreamRequest(h, streams) }, true)
	}
	if len(sessions) > 0 {
		c.request("endSessionUpdateRequest", func(h hsdi.RequestHeader) []byte { return hsdi.EndSessionUpdateRequest(h, sessions) }, true)
	}
	deadline := time.After(c.b.cfg.ShutdownTimeout)
	for {
		c.mu.Lock()
		n := len(c.pending)
		c.mu.Unlock()
		if n == 0 {
			return
		}
		select {
		case <-readErr:
			return
		case <-deadline:
			c.b.log.Warn("shutdown: responses not received", "outstanding", n)
			return
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func ids(v []int64) []string {
	out := make([]string, len(v))
	for i, id := range v {
		out[i] = strconv.FormatInt(id, 10)
	}
	return out
}

func (c *conn) checkLate(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for sqn, p := range c.pending {
		if !p.late && now.Sub(p.sentAt) > c.b.cfg.ResponseTimeout {
			p.late = true
			c.b.Stats.LateResponses.Add(1)
			c.b.log.Warn("response late", "request", p.root, "msgSQN", sqn, "after", now.Sub(p.sentAt).Round(time.Millisecond).String())
		}
	}
}

// request sends a control-channel request built by build and records it as
// waiting for a response.
func (c *conn) request(root string, build func(hsdi.RequestHeader) []byte, withID bool) error {
	_, err := c.send(root, build, withID, true)
	return err
}

// send writes one message. Requests get the next msgSQN and, if withID, the
// next requestID.
func (c *conn) send(root string, build func(hsdi.RequestHeader) []byte, withID, isRequest bool) (int64, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	now := time.Now()
	h := hsdi.RequestHeader{SessionID: c.clientSID, SessionType: hsdi.ClientSessionType, Chn: hsdi.ChnControl, ReplyTo: -1, Time: now}
	if isRequest {
		c.msgSQN++
		h.MsgSQN = c.msgSQN
		if withID {
			c.reqID++
			h.RequestID = c.reqID
		}
		c.mu.Lock()
		c.pending[h.MsgSQN] = &pending{root: root, requestID: h.RequestID, sentAt: now}
		c.mu.Unlock()
	}
	return h.RequestID, c.write(root, build(h))
}

// reply writes a response to a gateway message.
func (c *conn) reply(root string, doc []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.write(root, doc)
}

func (c *conn) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// write sends doc in a MIME message and publishes a copy. writeMu is held.
func (c *conn) write(root string, doc []byte) error {
	if c.isClosed() {
		return net.ErrClosed
	}
	c.nc.SetWriteDeadline(time.Now().Add(c.b.cfg.WriteTimeout))
	if _, err := c.nc.Write(mime.Encode(mime.NewBoundary(), mime.OutPart{ContentType: mime.TypeXML, Body: doc})); err != nil {
		return fmt.Errorf("write %s: %w", root, err)
	}
	c.b.Stats.Sent.Add(1)
	c.publish(context.Background(), c.newMsg(raw.DirOut, raw.Token(root, raw.TypeXML), doc, time.Now()), "")
	return nil
}

type countingReader struct {
	r io.Reader
	b *Bridge
}

func (cr countingReader) Read(p []byte) (int, error) {
	n, err := cr.r.Read(p)
	if n > 0 {
		cr.b.Stats.BytesIn.Add(int64(n))
		cr.b.lastRead.Store(time.Now().UnixNano())
	}
	return n, err
}

func (c *conn) readLoop(ctx context.Context) error {
	b := c.b
	r := mime.NewReader(countingReader{c.nc, b})
	for {
		msg, err := r.Next()
		var fe *mime.FramingError
		if err != nil && c.isClosed() {
			// We closed the socket; whatever the reader saw is not the
			// gateway's doing.
			return fmt.Errorf("read: %w", net.ErrClosed)
		}
		if errors.As(err, &fe) {
			b.Stats.FramingErrors.Add(1)
			b.Stats.DiscardedBytes.Add(int64(fe.Discarded))
			b.log.Warn("MIME framing error", "reason", fe.Reason, "partsKept", len(fe.Parts), "discarded", fe.Discarded)
			c.publishEvent(ctx, raw.EventFramingError, raw.Event{Reason: fe.Reason})
			c.publishDiscarded(ctx, fe.Discarded, fe.DiscardedData)
			c.handleParts(ctx, &mime.Message{Parts: fe.Parts}, time.Now())
			continue
		}
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
		if msg.Discarded > 0 {
			b.Stats.DiscardedBytes.Add(int64(msg.Discarded))
			b.log.Warn("bytes skipped before message", "discarded", msg.Discarded,
				"lines", bytes.Count(msg.DiscardedData, []byte{'\n'}), "headerNames", headerNames(msg.DiscardedData),
				"nextStartsWith", headerName(msg.FirstLine))
			c.publishDiscarded(ctx, msg.Discarded, msg.DiscardedData)
		}
		if msg.Bare {
			b.Stats.BareMessages.Add(1)
		} else {
			b.Stats.MIMEMessages.Add(1)
			b.Stats.noteHeaderLines(msg.HeaderLines)
		}
		c.handleParts(ctx, msg, time.Now())
	}
}

func (c *conn) handleParts(ctx context.Context, msg *mime.Message, received time.Time) {
	b := c.b
	for i, p := range msg.Parts {
		b.Stats.Parts.Add(1)
		b.Stats.noteHeaderLines(p.HeaderLines)
		ctype := p.ContentType()
		isXML := ctype == mime.TypeXML || (ctype != mime.TypeBinary && len(p.Body) > 0 && p.Body[0] == '<')
		typ, usual, state := raw.TypeBinary, mime.TypeBinary, ""
		var h hsdi.Header
		var err error
		if isXML {
			b.Stats.XMLParts.Add(1)
			usual = mime.TypeXML
			if h, err = hsdi.ParseHeader(p.Body); err != nil {
				b.Stats.XMLParseErrors.Add(1)
				b.log.Warn("XML header not readable", "err", err, "bytes", len(p.Body))
				typ = raw.TypeXML
			} else {
				typ = raw.Token(h.Root, raw.TypeXML)
				state, _ = raw.StateSubject(b.cfg.Name, h.Root, h.SessionID, h.Chn)
			}
		} else {
			b.Stats.BinaryParts.Add(1)
		}
		m := c.newMsg(raw.DirIn, typ, p.Body, received)
		if ctype != usual {
			m.Header.Set(raw.HType, p.Header.Get("Content-Type"))
		}
		if len(msg.Parts) > 1 {
			m.Header.Set(raw.HPart, fmt.Sprintf("%d/%d", i+1, len(msg.Parts)))
		}
		c.publish(ctx, m, state)
		if isXML && err == nil {
			b.Stats.noteRoot(h.Root)
			c.handle(h)
		}
	}
}

// handle acts on one gateway XML message.
func (c *conn) handle(h hsdi.Header) {
	b := c.b
	now := time.Now()
	if hsdi.NeedsAck(h.Root) {
		if err := c.reply("genericResponse", hsdi.GenericResponse(h, now)); err != nil {
			b.log.Warn("ack not sent", "to", h.Root, "err", err)
		}
	}
	switch h.Root {
	case "keepAliveRequest":
		if err := c.reply("keepAliveResponse", hsdi.KeepAliveResponse(h, now)); err != nil {
			b.log.Warn("keep-alive reply not sent", "err", err)
		}
	case "binEnd":
		b.Stats.BinEnds.Add(1)
		b.log.Info("stream ended by gateway", "device", devicePart(h.SessionID), "chn", h.Chn, "message", truncate(hsdi.Redact(h.Message), 200))
		if reRequestUseless(h.Message) {
			b.Stats.BinEndsNotReRequested.Add(1)
		} else {
			c.scheduleReRequest(h.SessionID, h.Chn)
		}
	case "binGapStart", "binGapEnd":
		b.log.Debug(h.Root, "device", devicePart(h.SessionID), "chn", h.Chn)
	case "helloResponse", "getSessionUpdateResponse", "getStreamResponse", "endStreamResponse",
		"endSessionUpdateResponse", "keepAliveResponse", "genericResponse":
		c.mu.Lock()
		p := c.pending[h.ReplyToMsgSQN]
		delete(c.pending, h.ReplyToMsgSQN)
		if p != nil && p.requestID > 0 && !h.IsNak() {
			switch p.root {
			case "getStreamRequest":
				c.streamReqIDs = append(c.streamReqIDs, p.requestID)
			case "getSessionUpdateRequest":
				c.sessReqIDs = append(c.sessReqIDs, p.requestID)
			}
		}
		c.mu.Unlock()
		if h.IsNak() {
			b.Stats.Naks.Add(1)
			b.log.Warn("request refused", "response", h.Root, "status", h.Status, "error", truncate(hsdi.Redact(h.ErrorString), 200))
		} else if h.Status != "" && h.Status != "ack" {
			b.log.Info("request partly accepted", "response", h.Root, "status", h.Status, "error", truncate(hsdi.Redact(h.ErrorString), 200))
		}
		if p == nil && h.Root != "genericResponse" {
			b.log.Debug("response to unknown request", "response", h.Root, "replyto", h.ReplyToMsgSQN)
		}
		if h.Root == "helloResponse" {
			select {
			case c.helloCh <- h:
			default:
			}
		}
	}
}

// reRequestUseless reports whether a <binEnd> reason is one where asking for
// the stream again is pointless. Real gateways refuse every re-request after
// "removed from data acquisition" with nak_unknownSessionID, and "unsubscription" answers our own end request.
// Other reasons, such as "Client too slow" (3-26), are still re-requested.
// The match is on the gateway's English text; if the text differs, the
// re-request is sent and refused, which is harmless.
func reRequestUseless(message string) bool {
	m := strings.ToLower(message)
	return strings.Contains(m, "removed from data acquisition") || strings.Contains(m, "unsubscription")
}

// scheduleReRequest asks for a session's stream again after a <binEnd>
// (3-25). Not done while shutting down, when <binEnd> answers our own
// <endStreamRequest>.
func (c *conn) scheduleReRequest(sessionID string, chn int) {
	b := c.b
	if b.cfg.ReRequestDelay < 0 || sessionID == "" {
		return
	}
	var sel string
	switch chn {
	case hsdi.ChnNumeric:
		sel = hsdi.SelectNumeric
	case hsdi.ChnWaveform:
		sel = hsdi.SelectWaveform
	default:
		b.log.Warn("binEnd on unexpected channel; not re-requesting", "chn", chn)
		return
	}
	key := sessionID + "/" + strconv.Itoa(chn)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.shuttingDown || c.closed || c.reRequests[key] != nil {
		return
	}
	c.reRequests[key] = time.AfterFunc(b.cfg.ReRequestDelay, func() {
		c.mu.Lock()
		delete(c.reRequests, key)
		stop := c.shuttingDown || c.closed
		c.mu.Unlock()
		if stop {
			return
		}
		b.Stats.ReRequests.Add(1)
		if err := c.request("getStreamRequest", func(h hsdi.RequestHeader) []byte {
			return hsdi.GetStreamRequest(h, []string{sessionID}, sel)
		}, true); err != nil {
			b.log.Warn("re-request not sent", "err", err)
			return
		}
		b.log.Info("stream requested again", "device", devicePart(sessionID), "chn", chn)
	})
}

// newMsg returns a raw stream message with the headers every message has.
func (c *conn) newMsg(dir, typ string, body []byte, t time.Time) *nats.Msg {
	m := nats.NewMsg(raw.Subject(c.b.cfg.Name, dir, typ))
	m.Data = body
	m.Header.Set(raw.HConn, c.id)
	m.Header.Set(raw.HReceived, t.UTC().Format(raw.TimeFormat))
	return m
}

// publishDiscarded puts n skipped bytes, of which data holds up to 1 MB, in
// the raw stream, where they can be inspected inside the PHI boundary.
func (c *conn) publishDiscarded(ctx context.Context, n int, data []byte) {
	if n == 0 {
		return
	}
	m := c.newMsg(raw.DirIn, raw.TypeDiscarded, data, time.Now())
	m.Header.Set(raw.HDiscarded, strconv.Itoa(n))
	c.publish(ctx, m, "")
}

var headerNameRE = regexp.MustCompile(`^([A-Za-z][A-Za-z0-9-]{0,40}):`)

// headerName returns the name of a MIME header line, or "" if the line does
// not look like one. Only the name is returned, never the value, so it can be
// logged.
func headerName(line string) string {
	if m := headerNameRE.FindStringSubmatch(line); m != nil {
		return m[1]
	}
	return ""
}

// headerNames lists the names of header-like lines in skipped data, and
// whether any line is a boundary, for the log.
func headerNames(data []byte) []string {
	var out []string
	for _, line := range strings.Split(string(data), "\n") {
		if n := headerName(line); n != "" {
			out = append(out, n)
		} else if strings.HasPrefix(line, "--") {
			out = append(out, "(boundary)")
		}
		if len(out) == 20 {
			break
		}
	}
	return out
}

func (c *conn) publishEvent(ctx context.Context, typ string, e raw.Event) {
	body, _ := json.Marshal(e)
	c.publish(ctx, c.newMsg(raw.DirEvent, typ, body, time.Now()), raw.StateEventSubject(c.b.cfg.Name, typ))
}

// publish assigns the next sequence number and publishes m to the raw stream,
// and a copy to stateSubject in the state stream unless it is empty. Holding
// pubMu keeps the order of publication the order of sequence numbers.
func (c *conn) publish(ctx context.Context, m *nats.Msg, stateSubject string) {
	c.pubMu.Lock()
	defer c.pubMu.Unlock()
	c.seq++
	m.Header.Set(raw.HSeq, strconv.FormatUint(c.seq, 10))
	var state *nats.Msg
	if stateSubject != "" {
		state = &nats.Msg{Subject: stateSubject, Header: maps.Clone(m.Header), Data: m.Data}
	}
	if err := c.b.pub.Publish(ctx, m); err != nil {
		c.b.Stats.PublishErrors.Add(1)
		c.b.log.Error("publish failed; message lost", "seq", c.seq, "err", err)
	}
	if state == nil {
		return
	}
	c.b.Stats.StateMessages.Add(1)
	if err := c.b.pub.Publish(ctx, state); err != nil {
		c.b.Stats.PublishErrors.Add(1)
		c.b.log.Error("state publish failed; message lost", "seq", c.seq, "subject", state.Subject, "err", err)
	}
}

// devicePart returns the device identifier of a session ID, without the
// session start time in its last four bytes (4-9), so logs carry no dates
// from the gateway.
func devicePart(sessionID string) string {
	if len(sessionID) == 24 {
		return sessionID[:16]
	}
	return "?"
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}
