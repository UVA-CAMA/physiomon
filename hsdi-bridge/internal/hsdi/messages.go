package hsdi

import (
	"encoding/xml"
	"fmt"
	"net"
	"strings"
	"time"
)

// Messages are written in one fixed layout, indented by 4, that the gateways
// accept. The manual's examples differ: they use version="0.0" on
// <sapphire>, and a gateway answered nothing to a hello written that way.
// The expected bytes of each message are in testdata/ and the tests compare
// against them byte for byte.

// Version is the <sapphire> version the gateways accept.
const Version = "2.0.1"

// Selectors for getStreamRequest: the two queries the manual says the
// gateway supports (3-19).
const (
	SelectNumeric  = "//params/*/(num|cfg)/*"
	SelectWaveform = "//params/*/wav/*"
)

// Data channels (3-9).
const (
	ChnControl  = 0
	ChnNumeric  = 1
	ChnWaveform = 2
)

// ClientSessionType is the type of the client's own session ID (4-10).
const ClientSessionType = "IP-32:PORT-32:NTPS-32"

// ClientSessionID builds the client's session ID: the gateway's IPv4
// address, its port as four bytes, and the current time in Unix seconds as
// four bytes, in hex. The manual calls this layout IP-32:PORT-32:NTPS-32
// (4-10), where the last field is NTP seconds; the gateways accept Unix
// seconds.
func ClientSessionID(gateway *net.TCPAddr, now time.Time) string {
	ip := gateway.IP.To4()
	if ip == nil {
		ip = net.IPv4zero.To4()
	}
	return strings.ToUpper(fmt.Sprintf("%02x%02x%02x%02x%08x%08x",
		ip[0], ip[1], ip[2], ip[3], uint32(gateway.Port), uint32(now.Unix())))
}

// RequestHeader is the header of a message the client sends.
type RequestHeader struct {
	SessionID string
	// SessionType is written as the sessionID's type attribute if set: on
	// the client's own session ID, not on IDs it echoes.
	SessionType string
	Chn         int
	MsgSQN      int64 // request; omitted if 0
	RequestID   int64 // request; omitted if 0
	ReplyTo     int64 // response; omitted if < 0
	// ResponseID is written for responses when HasResponseID is set.
	ResponseID    int64
	HasResponseID bool
	Time          time.Time
}

// writer builds indented XML the way JiBX does with indent 4.
type writer struct {
	b     strings.Builder
	depth int
}

func (w *writer) line(s string) {
	w.b.WriteString(strings.Repeat("    ", w.depth))
	w.b.WriteString(s)
	w.b.WriteByte('\n')
}

func (w *writer) open(s string)  { w.line(s); w.depth++ }
func (w *writer) close(s string) { w.depth--; w.line(s) }

// Time format: whole seconds, with fractional milliseconds only if non-zero,
// as JiBX writes java.util.Date.
func formatTime(t time.Time) string {
	return t.UTC().Truncate(time.Millisecond).Format("2006-01-02T15:04:05.999Z")
}

func (h RequestHeader) write(w *writer) {
	w.open(`<header xmlns="urn:ge:sapphire:base_1">`)
	if h.SessionType != "" {
		w.line(fmt.Sprintf(`<sessionID V="%s" type="%s"/>`, esc(h.SessionID), esc(h.SessionType)))
	} else {
		w.line(fmt.Sprintf(`<sessionID V="%s"/>`, esc(h.SessionID)))
	}
	w.line(fmt.Sprintf(`<msgCHN V="%d"/>`, h.Chn))
	if h.MsgSQN > 0 {
		w.line(fmt.Sprintf(`<msgSQN V="%d"/>`, h.MsgSQN))
	}
	if h.ReplyTo >= 0 {
		w.line(fmt.Sprintf(`<replytoMsgSQN V="%d"/>`, h.ReplyTo))
	}
	if h.RequestID > 0 {
		w.line(fmt.Sprintf(`<requestID V="%d"/>`, h.RequestID))
	}
	if h.HasResponseID {
		w.line(fmt.Sprintf(`<responseID V="%d" more="."/>`, h.ResponseID))
	}
	w.line(fmt.Sprintf(`<creationDateTime V="%s"/>`, formatTime(h.Time)))
	w.close(`</header>`)
}

// document writes a message element inside the <sapphire> root, with the
// message namespace on the message element.
func document(ns, element, attrs string, body func(w *writer)) []byte {
	w := &writer{}
	w.line(`<?xml version="1.0" encoding="UTF-8"?>`)
	w.open(`<s:sapphire xmlns:s="urn:ge:sapphire:sapphire_1" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" version="` + Version + `">`)
	w.open(`<` + element + attrs + ` xmlns="urn:ge:sapphire:` + ns + `">`)
	body(w)
	w.close(`</` + element + `>`)
	w.close(`</s:sapphire>`)
	return []byte(w.b.String())
}

// Hello is the first message on a connection (3-10). It has no requestID.
func Hello(h RequestHeader) []byte {
	h.RequestID, h.ReplyTo = 0, -1
	return document("helloMsg_1", "hello", "", h.write)
}

func query(w *writer, sessionIDs []string, sel string) {
	w.open(`<query xmlns="urn:ge:sapphire:base_1">`)
	w.line(`<endTime V="*"/>`)
	w.open(`<sessionIDList>`)
	for _, s := range sessionIDs {
		w.line(fmt.Sprintf(`<sessionID V="%s"/>`, esc(s)))
	}
	w.close(`</sessionIDList>`)
	w.line(fmt.Sprintf(`<selectNodes select="%s"/>`, esc(sel)))
	w.close(`</query>`)
}

// GetSessionUpdateRequest subscribes to session updates for all sessions
// (3-12, 3-33).
func GetSessionUpdateRequest(h RequestHeader) []byte {
	h.ReplyTo = -1
	return document("sessionUpdateMsg_1", "getSessionUpdateRequest", "", func(w *writer) {
		h.write(w)
		query(w, []string{"*"}, "//*")
	})
}

// GetStreamRequest requests numeric and configuration data or waveform data
// for the given sessions, or "*" for all current and future sessions (3-19).
func GetStreamRequest(h RequestHeader, sessionIDs []string, sel string) []byte {
	h.ReplyTo = -1
	return document("getStreamMsg_1", "getStreamRequest", "", func(w *writer) {
		h.write(w)
		query(w, sessionIDs, sel)
	})
}

func requestIDList(w *writer, ids []string) {
	w.open(`<requestIDList>`)
	for _, id := range ids {
		w.line(fmt.Sprintf(`<originalRequestID V="%s"/>`, esc(id)))
	}
	w.close(`</requestIDList>`)
}

// EndStreamRequest ends streams by original request ID, or "*" for all (3-21).
func EndStreamRequest(h RequestHeader, ids []string) []byte {
	h.ReplyTo = -1
	return document("endStreamMsg_1", "endStreamRequest", "", func(w *writer) {
		h.write(w)
		requestIDList(w, ids)
	})
}

// EndSessionUpdateRequest ends session subscriptions (3-17).
func EndSessionUpdateRequest(h RequestHeader, ids []string) []byte {
	h.ReplyTo = -1
	return document("sessionUpdateMsg_1", "endSessionUpdateRequest", "", func(w *writer) {
		h.write(w)
		requestIDList(w, ids)
	})
}

// GenericResponse acknowledges a gateway message (3-28). The header echoes the acknowledged message's sessionID and msgCHN, its msgSQN as
// replytoMsgSQN, and its responseID, with more=".".
func GenericResponse(to Header, now time.Time) []byte {
	h := RequestHeader{SessionID: to.SessionID, Chn: max(to.Chn, 0), ReplyTo: to.MsgSQN, Time: now}
	if to.ResponseID >= 0 {
		h.ResponseID, h.HasResponseID = to.ResponseID, true
	}
	return document("genericMsg_1", "genericResponse", ` status="ack"`, h.write)
}

// KeepAliveRequest checks that the connection is alive (3-27).
func KeepAliveRequest(h RequestHeader) []byte {
	h.RequestID, h.ReplyTo = 0, -1
	return document("keepAliveMsg_1", "keepAliveRequest", "", h.write)
}

// KeepAliveResponse answers a keep-alive request (3-27), in GenericResponse's
// form. A real gateway's request carries a
// requestID, which the manual's example lacks; it is echoed as responseID, as
// every other response does.
func KeepAliveResponse(to Header, now time.Time) []byte {
	h := RequestHeader{SessionID: to.SessionID, Chn: max(to.Chn, 0), ReplyTo: to.MsgSQN, Time: now}
	if to.RequestID >= 0 {
		h.ResponseID, h.HasResponseID = to.RequestID, true
	}
	return document("keepAliveMsg_1", "keepAliveResponse", ` status="ack"`, h.write)
}

// NeedsAck reports whether the client must reply with a genericResponse
// (3-15, 3-23 to 3-26).
func NeedsAck(root string) bool {
	switch root {
	case "sessionUpdate", "binHeader", "binDescriptor", "binGapStart", "binGapEnd", "binEnd":
		return true
	}
	return false
}

func esc(s string) string {
	var b strings.Builder
	xml.EscapeText(&b, []byte(s))
	return strings.ReplaceAll(b.String(), `"`, "&#34;")
}
