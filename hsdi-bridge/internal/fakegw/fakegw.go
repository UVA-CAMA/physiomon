// Package fakegw is a stand-in for an HSDI gateway, for testing the
// bridge without a real gateway and without patient data.
//
// It answers the client's requests the way the manual describes, sends made-up
// session updates, descriptors and binary frames, and can inject the faults a
// client must survive: long MIME headers, garbage between messages, a dropped
// connection, a <binEnd>, and silence.
//
// The descriptors and frames are simple and fixed. They exercise the bridge,
// which does not read them; they are not a model of real gateway data.
package fakegw

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/UVA-CAMA/physiomon/hsdi-bridge/internal/hsdi"
	"github.com/UVA-CAMA/physiomon/hsdi-bridge/internal/mime"
)

// Options controls what the fake gateway sends. Counts of frames are per
// connection and count waveform frames of all sessions.
type Options struct {
	Sessions        int           // fake beds; default 2
	WaveInterval    time.Duration // waveform frame interval per session; default 250 ms (4 Hz, 4-22)
	NumericInterval time.Duration // default 2 s (4-24)
	// Extra header lines added to every part, to test more than ten lines.
	ExtraHeaderLines int
	GarbageAfter     int // write a line of garbage after this many waveform frames, once
	BinEndAfter      int // end the first session's waveform stream after this many frames, once per connection
	// BinEndMessage is the reason in that <binEnd>; default "Client too slow."
	BinEndMessage string
	// BareKeepAliveEvery sends a <keepAliveRequest> this often as XML with
	// no MIME framing, as a real gateway does.
	BareKeepAliveEvery time.Duration
	// NakSessionRequests refuses stream requests for named sessions with
	// nak_unknownSessionID, as a real gateway did for every re-request.
	NakSessionRequests bool
	DropAfter          int  // close the first connection after this many frames
	IgnoreKeepAlive    bool // do not answer keep-alive requests
	// IgnoreHello accepts connections but never answers <hello>, as a real
	// gateway did for an hour after a stall.
	IgnoreHello bool
	Silent      bool // answer requests but send no data
	Log         *slog.Logger
}

// Received is a message the fake gateway received from the client.
type Received struct {
	Conn   int
	Header hsdi.Header
	Body   string
}

// Server is a running fake gateway.
type Server struct {
	ln    net.Listener
	opts  Options
	conns atomic.Int64

	mu       sync.Mutex
	received []Received
	closed   bool
	open     map[net.Conn]bool
}

// Start listens on addr ("127.0.0.1:0" for any free port) and serves.
func Start(addr string, opts Options) (*Server, error) {
	if opts.Sessions == 0 {
		opts.Sessions = 2
	}
	if opts.WaveInterval == 0 {
		opts.WaveInterval = 250 * time.Millisecond
	}
	if opts.NumericInterval == 0 {
		opts.NumericInterval = 2 * time.Second
	}
	if opts.Log == nil {
		opts.Log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	s := &Server{ln: ln, opts: opts, open: map[net.Conn]bool{}}
	go s.accept()
	return s, nil
}

// Addr returns the listening address.
func (s *Server) Addr() string { return s.ln.Addr().String() }

// Connections returns how many connections have been accepted.
func (s *Server) Connections() int { return int(s.conns.Load()) }

// Close stops listening and closes open connections.
func (s *Server) Close() {
	s.mu.Lock()
	s.closed = true
	for c := range s.open {
		c.Close()
	}
	s.mu.Unlock()
	s.ln.Close()
}

// Received returns the messages received so far.
func (s *Server) Received() []Received {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Received(nil), s.received...)
}

// Count returns how many received messages have the given type.
func (s *Server) Count(root string) int {
	n := 0
	for _, r := range s.Received() {
		if r.Header.Root == root {
			n++
		}
	}
	return n
}

// WaitFor waits until at least n messages of type root have been received.
func (s *Server) WaitFor(root string, n int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if s.Count(root) >= n {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func (s *Server) accept() {
	for {
		c, err := s.ln.Accept()
		if err != nil {
			return
		}
		n := int(s.conns.Add(1))
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			c.Close()
			return
		}
		s.open[c] = true
		s.mu.Unlock()
		go s.serve(c, n)
	}
}

// session is one fake bed.
type session struct {
	id    string
	index int
}

type gwConn struct {
	s      *Server
	c      net.Conn
	n      int
	sid    string // gateway's own session ID, used in response headers
	wmu    sync.Mutex
	sqn    [3]int64 // msgSQN per channel
	frames atomic.Int64
	done   chan struct{}
	once   sync.Once

	smu     sync.Mutex
	streams map[string]chan struct{} // session/chn -> stop
	ended   bool                     // BinEndAfter already used
	garbage bool
}

func (s *Server) serve(c net.Conn, n int) {
	g := &gwConn{s: s, c: c, n: n, sid: "7EC46464000007D74A3AAC99", done: make(chan struct{}), streams: map[string]chan struct{}{}}
	defer g.close()
	r := mime.NewReader(c)
	for {
		msg, err := r.Next()
		if err != nil {
			return
		}
		for _, p := range msg.Parts {
			h, err := hsdi.ParseHeader(p.Body)
			if err != nil {
				s.opts.Log.Warn("fakegw: unreadable client message", "err", err)
				continue
			}
			s.mu.Lock()
			s.received = append(s.received, Received{Conn: n, Header: h, Body: string(p.Body)})
			s.mu.Unlock()
			g.respond(h, string(p.Body))
		}
	}
}

func (g *gwConn) close() {
	g.once.Do(func() {
		close(g.done)
		g.c.Close()
		g.s.mu.Lock()
		delete(g.s.open, g.c)
		g.s.mu.Unlock()
	})
}

func (g *gwConn) sessions() []session {
	var out []session
	for i := 0; i < g.s.opts.Sessions; i++ {
		// Device part in the IP-32:PORT-32 layout, then a fixed fake time.
		out = append(out, session{id: fmt.Sprintf("7E01E1%02X000007D04A3AAC85", 0x10+i), index: i})
	}
	return out
}

// send writes one gateway message on channel chn with a fresh msgSQN.
func (g *gwConn) send(chn int, root, attrs, inner string, responseID int64, more string) {
	g.wmu.Lock()
	g.sqn[chn]++
	sqn := g.sqn[chn]
	g.wmu.Unlock()
	hdr := fmt.Sprintf(`<header xmlns="urn:ge:sapphire:base_1"><sessionID V="%s"/><msgCHN V="%d"/><msgSQN V="%d"/>`, g.sid, chn, sqn)
	if responseID > 0 {
		hdr += fmt.Sprintf(`<responseID V="%d" more="%s"/>`, responseID, more)
	}
	hdr += `<creationDateTime V="2000-01-01T00:00:00.000Z"/></header>`
	g.writeXML(doc(root, attrs, hdr+inner))
}

// sendFor is send with the header sessionID set to a device session, as on
// data-channel messages (3-23 to 3-26).
func (g *gwConn) sendFor(sess string, chn int, root, attrs, inner string, responseID int64, more string) {
	g.wmu.Lock()
	g.sqn[chn]++
	sqn := g.sqn[chn]
	g.wmu.Unlock()
	hdr := fmt.Sprintf(`<header xmlns="urn:ge:sapphire:base_1"><sessionID V="%s"/><msgCHN V="%d"/><msgSQN V="%d"/><responseID V="%d" more="%s"/><creationDateTime V="2000-01-01T00:00:00.000Z"/></header>`,
		sess, chn, sqn, responseID, more)
	g.writeXML(doc(root, attrs, hdr+inner))
}

func (g *gwConn) reply(to hsdi.Header, root, status, inner string) {
	hdr := fmt.Sprintf(`<header xmlns="urn:ge:sapphire:base_1"><sessionID V="%s"/><msgCHN V="0"/><replytoMsgSQN V="%d"/>`, to.SessionID, to.MsgSQN)
	if to.RequestID > 0 {
		hdr += fmt.Sprintf(`<responseID V="%d" more="*"/>`, to.RequestID)
	}
	hdr += `<creationDateTime V="2000-01-01T00:00:00.000Z"/></header>`
	g.writeXML(doc(root, fmt.Sprintf(` status="%s"`, status), hdr+inner))
}

// bareKeepAlive is a real gateway's keep-alive request, sent without MIME
// framing.
const bareKeepAlive = `<?xml version="1.0" encoding="UTF-8"?>
<s:sapphire xmlns:s="urn:ge:sapphire:sapphire_1" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" version="2.0.1">
 <keepAliveRequest xmlns="urn:ge:sapphire:keepAliveMsg_1">
  <header xmlns="urn:ge:sapphire:base_1">
   <sessionID V="%s" type="IP-32:PORT-32:NTPS-32"/>
   <msgCHN V="0"/>
   <msgSQN V="%d"/>
   <requestID V="%d"/>
   <creationDateTime V="2000-01-01T00:00:00.000Z"/>
  </header>
 </keepAliveRequest>
</s:sapphire>
`

func (g *gwConn) bareKeepAlives(client string) {
	t := time.NewTicker(g.s.opts.BareKeepAliveEvery)
	defer t.Stop()
	for id := 1; ; id++ {
		select {
		case <-g.done:
			return
		case <-t.C:
		}
		g.wmu.Lock()
		g.sqn[0]++
		sqn := g.sqn[0]
		g.wmu.Unlock()
		g.write(fmt.Appendf(nil, bareKeepAlive, client, sqn, 15000+id))
	}
}

func doc(root, attrs, inner string) []byte {
	return []byte(`<?xml version="1.0" encoding="UTF-8"?>` + "\r\n" +
		`<s:sapphire xmlns:s="urn:ge:sapphire:sapphire_1" xmlns="urn:ge:sapphire:fake_1" version="0.0">` +
		`<` + root + attrs + `>` + inner + `</` + root + `></s:sapphire>`)
}

func (g *gwConn) extra() []string {
	var x []string
	for i := 0; i < g.s.opts.ExtraHeaderLines; i++ {
		x = append(x, fmt.Sprintf("X-Fake-Header-%d: %d", i+1, i+1))
	}
	return x
}

func (g *gwConn) writeXML(body []byte) {
	g.write(mime.Encode(mime.NewBoundary(), mime.OutPart{ContentType: mime.TypeXML, Body: body, Extra: g.extra()}))
}

func (g *gwConn) write(b []byte) {
	g.wmu.Lock()
	defer g.wmu.Unlock()
	g.c.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := g.c.Write(b); err != nil {
		g.close()
	}
}

func (g *gwConn) respond(h hsdi.Header, body string) {
	switch h.Root {
	case "hello":
		if g.s.opts.IgnoreHello {
			return
		}
		g.reply(h, "helloResponse", "ack", `<device><globalID V="FFFFFFFFFFFFFFFF"/><vendorID V="FAKE"/><modelID V="Fake Gateway"/></device>`)
		if g.s.opts.BareKeepAliveEvery > 0 {
			go g.bareKeepAlives(h.SessionID)
		}
	case "getSessionUpdateRequest":
		g.reply(h, "getSessionUpdateResponse", "ack", "")
		g.send(0, "sessionUpdate", "", g.sessionList(), h.RequestID, "*")
	case "getStreamRequest":
		chn := hsdi.ChnNumeric
		if strings.Contains(body, "/wav/") {
			chn = hsdi.ChnWaveform
		}
		if g.s.opts.NakSessionRequests && !strings.Contains(body, `<sessionID V="*"/>`) {
			g.reply(h, "getStreamResponse", "nak", fmt.Sprintf(`<newMsgCHN V="%d"/>`, chn))
			return
		}
		g.reply(h, "getStreamResponse", "ack", fmt.Sprintf(`<newMsgCHN V="%d"/>`, chn))
		if !g.s.opts.Silent {
			g.startStreams(h, body, chn)
		}
	case "endStreamRequest":
		g.reply(h, "endStreamResponse", "ack", "")
		g.stopStreams(h.RequestID)
	case "endSessionUpdateRequest":
		g.reply(h, "endSessionUpdateResponse", "ack", "")
	case "keepAliveRequest":
		if !g.s.opts.IgnoreKeepAlive {
			g.reply(h, "keepAliveResponse", "ack", "")
		}
	}
}

func (g *gwConn) sessionList() string {
	var b strings.Builder
	b.WriteString(`<sessionList xmlns="urn:ge:sapphire:base_1">`)
	for _, s := range g.sessions() {
		fmt.Fprintf(&b, `<session><sessionID V="%s" type="IP-32:PORT-32:NTPS-32"/><startTime V="2000-01-01T00:00:00.000Z"/>`+
			`<deviceStatus><assignedLocation><unitName V="FAKEUNIT"/><bedName V="BED%d"/></assignedLocation>`+
			`<state><network V="ON_LINE"/><patientStatus V="ADMIT"/></state></deviceStatus>`+
			`<patientInfo><name><given V="TEST"/><family V="PATIENT%d"/></name><identifier><id V="FAKE%04d"/></identifier></patientInfo></session>`,
			s.id, s.index+1, s.index+1, s.index+1)
	}
	b.WriteString(`</sessionList>`)
	return b.String()
}

func (g *gwConn) startStreams(req hsdi.Header, body string, chn int) {
	var targets []session
	for _, s := range g.sessions() {
		if strings.Contains(body, `"*"`) || strings.Contains(body, s.id) {
			targets = append(targets, s)
		}
	}
	g.send(chn, "binHeader", "", `<startDateTime V="2000-01-01T00:00:00.000Z"><accuracy V="120" U="s"/></startDateTime><binaryFormat><byteOrder V="bigEndian"/></binaryFormat>`, req.RequestID, "*")
	for _, s := range targets {
		key := fmt.Sprintf("%s/%d", s.id, chn)
		g.smu.Lock()
		if old := g.streams[key]; old != nil {
			close(old)
		}
		stop := make(chan struct{})
		g.streams[key] = stop
		g.smu.Unlock()
		go g.stream(s, chn, req.RequestID, stop)
	}
}

func (g *gwConn) stopStreams(reqID int64) {
	g.smu.Lock()
	defer g.smu.Unlock()
	for key, stop := range g.streams {
		close(stop)
		delete(g.streams, key)
		sess, chnS, _ := strings.Cut(key, "/")
		chn := hsdi.ChnNumeric
		if chnS == "2" {
			chn = hsdi.ChnWaveform
		}
		go g.sendFor(sess, chn, "binEnd", "", `<block><formatID V="1"/><blockSQN V="1"/></block>`, reqID, ".")
	}
}

// Descriptor and frame layout: the block header fields of 4-21, then one
// signal of xs:short. Waveform: 60 samples of ECG lead II; numeric: heart rate.
func (g *gwConn) descriptor(formatID uint32, chn int) string {
	signal := `<ecg source="ge:unityDevice" index="1"><wav><ecgWaveformMXG><ecgWaveform lead="II" BT="xs:short" asizeBT="60" S="2.44" U="uV" INV="-32768"/></ecgWaveformMXG></wav></ecg>`
	rate := `rate="4" U="Hz"`
	if chn == hsdi.ChnNumeric {
		signal = `<ecg source="ge:unityDevice" index="1"><num><hr BT="xs:short" U="{beat}/min" INV="-32768"/></num></ecg>`
		rate = `rate="0.5" U="Hz"`
	}
	return fmt.Sprintf(`<block %s repeat="1"><formatID BT="xs:unsignedInt" iBV="%d"/><sessionID BT="xs:unsignedByte" asizeBT="12"/>`+
		`<blockSQN BT="xs:unsignedInt" iBV="1"/><blockLength BT="xs:unsignedShort"/><startDateTime BT="xb:NTP-32"/>`+
		`<groupBT repeat="1"><params xmlns="urn:ge:sapphire:param_1">%s</params></groupBT></block>`, rate, formatID&0x3FFFFFFF, signal)
}

func frame(formatID uint32, sessHex string, sqn uint32, samples int) []byte {
	n := 4 + 12 + 4 + 2 + 4 + 2*samples
	b := make([]byte, n)
	binary.BigEndian.PutUint32(b[0:], formatID)
	sid, _ := hex.DecodeString(sessHex)
	copy(b[4:16], sid)
	binary.BigEndian.PutUint32(b[16:], sqn)
	binary.BigEndian.PutUint16(b[20:], uint16(n))
	binary.BigEndian.PutUint32(b[22:], 3155673600) // 2000-01-01 in NTP seconds
	for i := 0; i < samples; i++ {
		binary.BigEndian.PutUint16(b[26+2*i:], uint16(int16(100*((i%20)-10))))
	}
	return b
}

func (g *gwConn) stream(s session, chn int, reqID int64, stop chan struct{}) {
	o := g.s.opts
	formatID := uint32(0x40000000) | uint32(1+2*s.index+chn-1)
	g.sendFor(s.id, chn, "binDescriptor", "", g.descriptor(formatID, chn), reqID, "*")
	interval, samples := o.WaveInterval, 60
	if chn == hsdi.ChnNumeric {
		interval, samples = o.NumericInterval, 1
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for sqn := uint32(1); ; sqn++ {
		select {
		case <-stop:
			return
		case <-g.done:
			return
		case <-t.C:
		}
		g.write(mime.Encode(mime.NewBoundary(), mime.OutPart{ContentType: mime.TypeBinary, Body: frame(formatID, s.id, sqn, samples), Extra: g.extra()}))
		if chn != hsdi.ChnWaveform {
			continue
		}
		n := int(g.frames.Add(1))
		g.smu.Lock()
		doGarbage := o.GarbageAfter > 0 && n >= o.GarbageAfter && !g.garbage
		if doGarbage {
			g.garbage = true
		}
		doEnd := o.BinEndAfter > 0 && n >= o.BinEndAfter && s.index == 0 && !g.ended
		if doEnd {
			g.ended = true
			delete(g.streams, fmt.Sprintf("%s/%d", s.id, chn))
		}
		g.smu.Unlock()
		if doGarbage {
			g.write([]byte("this is not a MIME message\r\n\x00\x01\x02\r\n"))
		}
		if o.DropAfter > 0 && n >= o.DropAfter && g.n == 1 {
			g.close()
			return
		}
		if doEnd {
			msg := o.BinEndMessage
			if msg == "" {
				msg = "Client too slow."
			}
			g.sendFor(s.id, chn, "binEnd", fmt.Sprintf(` message="%s"`, msg), `<block><formatID V="1"/><blockSQN V="1"/></block>`, reqID, ".")
			return
		}
	}
}
