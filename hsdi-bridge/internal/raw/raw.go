// Package raw defines the raw and state streams: their subjects, and the NATS
// headers the bridge puts on each message. The decoder imports it, so both
// programs agree.
//
// Each NATS message is one MIME part as received from the gateway, with the
// part body unchanged as the NATS payload. Messages the bridge sends, and
// events such as connects and disconnects, go in the same stream. The subject
// says which gateway, which direction and what type:
//
//	hsdi.raw.<gateway>.in.binary
//	hsdi.raw.<gateway>.in.<XML message type>   such as in.binDescriptor
//	hsdi.raw.<gateway>.in.xml                  XML whose header could not be read
//	hsdi.raw.<gateway>.in.discarded            bytes skipped while resynchronising
//	hsdi.raw.<gateway>.out.<XML message type>  sent by the bridge
//	hsdi.raw.<gateway>.event.<event type>      such as event.connected
//
// A stream numbers messages across all its subjects, so a consumer of
// hsdi.raw.<gateway>.> gets the gateway's history in order.
//
// The raw stream has an age limit, but a session's descriptor arrives once,
// when its stream starts, and a patient can stay connected for longer. So the
// bridge also publishes a copy of every message a reader needs to interpret
// later frames to the state stream, which has no age limit:
//
//	hsdi.state.<gateway>.binDescriptor.<session>.<channel>
//	hsdi.state.<gateway>.binHeader.<session>.<channel>
//	hsdi.state.<gateway>.binEnd.<session>.<channel>
//	hsdi.state.<gateway>.sessionUpdate
//	hsdi.state.<gateway>.event.<event type>
//
// Copies have the raw message's headers, so a reader can place them among the
// raw messages by Hsdi-Conn and Hsdi-Seq. The bridge reads only the XML header
// of each message.
//
// Nothing the subject or the body already says is repeated in a header.
package raw

import (
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Defaults for the streams the bridge creates if they do not exist.
const (
	DefaultStream  = "HSDI_RAW"
	SubjectPrefix  = "hsdi.raw"
	SubjectsFilter = SubjectPrefix + ".>"

	DefaultStateStream  = "HSDI_STATE"
	StatePrefix         = "hsdi.state"
	StateSubjectsFilter = StatePrefix + ".>"
)

// Headers. Every message has HConn, HSeq and HReceived; the others only when
// they apply.
const (
	HConn     = "Hsdi-Conn"     // connection ID, unique per TCP connection
	HSeq      = "Hsdi-Seq"      // message number within the connection, from 1, no gaps
	HReceived = "Hsdi-Received" // when the bridge read the MIME message, or wrote the message or event; RFC 3339 UTC
	// The part's Content-Type, only if it is not the usual one for the
	// subject's type: application/x-sapphire+binary for in.binary,
	// application/x-sapphire+xml for XML.
	HType = "Content-Type"
	// "n/m": part n of a MIME message with m parts, only if m > 1.
	HPart = "Hsdi-Part"
	// On in.discarded messages: the number of bytes skipped. The body holds
	// them, up to 1 MB, one line per line read.
	HDiscarded = "Hsdi-Discarded"
)

// Directions, the subject token after the gateway.
const (
	DirIn    = "in"    // from the gateway
	DirOut   = "out"   // sent by the bridge
	DirEvent = "event" // written by the bridge about the connection
)

// Types that are not XML message types.
const (
	TypeBinary    = "binary"
	TypeXML       = "xml" // XML whose header could not be read
	TypeDiscarded = "discarded"
)

// Event types.
const (
	EventConnected    = "connected"    // TCP connection open
	EventDisconnected = "disconnected" // TCP connection closed; body has the reason
	EventFramingError = "framing-error"
)

// Event is the JSON body of an event message. Its type is in the subject.
type Event struct {
	Reason string `json:"reason,omitempty"`
	Remote string `json:"remote,omitempty"`
}

var validName = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// ValidGatewayName reports whether name can be used as a subject token.
func ValidGatewayName(name string) bool { return validName.MatchString(name) }

// Token returns s if it can be used as a subject token, else alt.
func Token(s, alt string) string {
	if validName.MatchString(s) {
		return s
	}
	return alt
}

// Subject returns the raw subject for a message.
func Subject(gateway, dir, typ string) string {
	return SubjectPrefix + "." + gateway + "." + dir + "." + typ
}

// GatewayFilter returns the subject filter for one gateway's raw messages.
func GatewayFilter(gateway string) string { return SubjectPrefix + "." + gateway + ".>" }

// Name is a raw subject split into its parts.
type Name struct {
	Gateway, Dir, Type string
}

// ParseSubject splits a raw subject.
func ParseSubject(subject string) (Name, bool) {
	f := strings.Split(subject, ".")
	if len(f) != 5 || f[0]+"."+f[1] != SubjectPrefix {
		return Name{}, false
	}
	return Name{Gateway: f[2], Dir: f[3], Type: f[4]}, true
}

// IsXML reports whether the message is XML, from the gateway or the bridge.
func (n Name) IsXML() bool {
	return n.Dir == DirOut || (n.Dir == DirIn && n.Type != TypeBinary && n.Type != TypeDiscarded)
}

// StateRoots are the gateway messages copied to the state stream. Those with
// true are per session and channel.
var StateRoots = map[string]bool{
	"binDescriptor": true, "binHeader": true, "binEnd": true, "sessionUpdate": false,
}

// StateSubject returns the state subject for a gateway message of type root,
// and false if it does not belong in the state stream. chn < 0 means unknown.
func StateSubject(gateway, root, session string, chn int) (string, bool) {
	perSession, ok := StateRoots[root]
	if !ok {
		return "", false
	}
	s := StatePrefix + "." + gateway + "." + root
	if !perSession {
		return s, true
	}
	c := "_"
	if chn >= 0 {
		c = strconv.Itoa(chn)
	}
	return s + "." + Token(session, "_") + "." + c, true
}

// StateEventSubject returns the state subject for an event.
func StateEventSubject(gateway, event string) string {
	return StatePrefix + "." + gateway + "." + DirEvent + "." + event
}

// TimeFormat is the format of HReceived.
const TimeFormat = time.RFC3339Nano

// StreamConfig returns the configuration the bridge uses when it creates the
// raw stream: file storage, compressed. MaxBytes counts bytes before
// compression; on real traffic the files took about a third of that.
func StreamConfig(name string, maxAge time.Duration, maxBytes int64) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:        name,
		Description: "Raw MIME parts from HSDI gateways, written by hsdi2nats",
		Subjects:    []string{SubjectsFilter},
		Storage:     jetstream.FileStorage,
		Compression: jetstream.S2Compression,
		Retention:   jetstream.LimitsPolicy,
		Discard:     jetstream.DiscardOld,
		MaxAge:      maxAge,
		MaxBytes:    maxBytes,
	}
}

// StateStreamConfig returns the configuration the bridge uses when it creates
// the state stream. No age limit: a descriptor is needed for as long as its
// session sends frames.
func StateStreamConfig(name string, maxBytes int64) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:        name,
		Description: "Descriptors, session updates and connection events from HSDI gateways, kept without an age limit, written by hsdi2nats",
		Subjects:    []string{StateSubjectsFilter},
		Storage:     jetstream.FileStorage,
		Compression: jetstream.S2Compression,
		Retention:   jetstream.LimitsPolicy,
		Discard:     jetstream.DiscardOld,
		MaxBytes:    maxBytes,
	}
}
