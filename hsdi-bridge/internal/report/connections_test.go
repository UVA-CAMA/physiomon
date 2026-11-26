package report

import (
	"bytes"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/UVA-CAMA/physiomon/hsdi-bridge/internal/raw"
)

func TestConnections(t *testing.T) {
	c := NewCollector(true)
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	add := func(conn, subject string, at time.Duration, body string) {
		h := nats.Header{}
		h.Set(raw.HConn, conn)
		h.Set(raw.HReceived, t0.Add(at).Format(raw.TimeFormat))
		c.Add(subject, h, []byte(body))
	}
	add("a1", "hsdi.raw.A.event.connected", 0, `{}`)
	add("a1", "hsdi.raw.A.in.helloResponse", time.Second, `<helloResponse/>`)
	for i := range 4 {
		add("a1", "hsdi.raw.A.in.keepAliveRequest", time.Duration(i+1)*time.Minute, `<keepAliveRequest/>`)
	}
	add("a1", "hsdi.raw.A.out.keepAliveRequest", 5*time.Minute, `<keepAliveRequest/>`)
	add("a1", "hsdi.raw.A.event.disconnected", 5*time.Minute+10*time.Second, `{"reason":"no reply to keep-alive in 10s"}`)
	for i := range 3 {
		conn := "a" + strconv.Itoa(i+2)
		at := 6*time.Minute + time.Duration(i)*time.Minute
		add(conn, "hsdi.raw.A.event.connected", at, `{}`)
		add(conn, "hsdi.raw.A.event.disconnected", at+10*time.Second, `{"reason":"no hello response in 10s"}`)
	}
	// Another gateway's first connection has the same ID.
	add("a1", "hsdi.raw.B.event.connected", 0, `{}`)
	add("a1", "hsdi.raw.B.in.helloResponse", time.Second, `<helloResponse/>`)
	add("a1", "hsdi.raw.B.in.binary", 2*time.Hour, "")
	var b bytes.Buffer
	c.writeConns(&b)
	out := b.String()
	for _, want := range []string{
		"  A\n",
		"+0s .. +5m10s  5m10s, 0 frames, gateway keep-alives 4 every 1m0s (1m0s to 1m0s), ours 1 answered 0, ended: no reply to keep-alive in 10s\n",
		"+6m0s .. +8m10s  3 connection(s) without a hello reply, ended: no hello response in 10s\n",
		"  B\n    +0s .. +2h0m0s  2h0m0s, 1 frames, gateway keep-alives 0, ours 0 answered 0, ended: still open\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in\n%s", want, out)
		}
	}
	if strings.Contains(out, "2026") {
		t.Errorf("printed a date:\n%s", out)
	}
}
