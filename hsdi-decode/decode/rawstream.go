package decode

import (
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

// The names hsdi2nats writes, as its README documents them ("The raw stream",
// "The state stream"). They are copied here so that the decoder does not
// depend on the bridge's code; the README is the contract between the two.
const (
	RawStream   = "HSDI_RAW"
	StateStream = "HSDI_STATE"

	rawPrefix      = "hsdi.raw"   // hsdi.raw.<gateway>.<in|out|event>.<type>
	statePrefix    = "hsdi.state" // hsdi.state.<gateway>.<type>[.<session>.<channel>]
	headerReceived = "Hsdi-Received"
	typeBinary     = "binary"
)

// rawSubject is hsdi.raw.<gateway>.in.<typ>.
func rawSubject(gateway, typ string) string {
	return rawPrefix + "." + gateway + ".in." + typ
}

// parseSubject returns the gateway and message type of a raw or state
// subject: hsdi.raw.<gateway>.in.<type> or hsdi.state.<gateway>.<type>....
func parseSubject(subject string) (gateway, typ string, ok bool) {
	tok := strings.Split(subject, ".")
	switch {
	case len(tok) == 5 && tok[0]+"."+tok[1] == rawPrefix && tok[3] == "in":
		return tok[2], tok[4], true
	case len(tok) >= 4 && tok[0]+"."+tok[1] == statePrefix:
		return tok[2], tok[3], true
	}
	return "", "", false
}

func received(h nats.Header) time.Time {
	t, _ := time.Parse(time.RFC3339Nano, h.Get(headerReceived))
	return t
}
