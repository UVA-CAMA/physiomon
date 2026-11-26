package report

import (
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/UVA-CAMA/physiomon/hsdi-bridge/internal/raw"
)

// connStat is one TCP connection's history, from its messages. Times are
// printed as offsets from the start of the report window, never as dates.
type connStat struct {
	gateway     string
	first, last time.Time
	hello       bool // helloResponse received
	frames      int
	gwKeepAlive []time.Time // keepAliveRequests from the gateway
	ours        int         // keepAliveRequests we sent
	replies     int         // keepAliveResponses from the gateway
	end         string      // disconnect reason, "" if still open
}

func (c *Collector) addConn(n raw.Name, h nats.Header, t time.Time, data []byte) {
	// Connection IDs are unique within a gateway only.
	id := h.Get(raw.HConn)
	if id == "" || t.IsZero() {
		return
	}
	id = n.Gateway + " " + id
	cs := c.conns[id]
	if cs == nil {
		cs = &connStat{gateway: n.Gateway, first: t}
		c.conns[id] = cs
	}
	cs.first = minTime(cs.first, t)
	if t.After(cs.last) {
		cs.last = t
	}
	switch {
	case n.Dir == raw.DirEvent && n.Type == raw.EventDisconnected:
		var e raw.Event
		json.Unmarshal(data, &e)
		cs.end = e.Reason
	case n.Dir == raw.DirIn && n.Type == raw.TypeBinary:
		cs.frames++
	case n.Dir == raw.DirIn && n.Type == "helloResponse":
		cs.hello = true
	case n.Dir == raw.DirIn && n.Type == "keepAliveRequest":
		cs.gwKeepAlive = append(cs.gwKeepAlive, t)
	case n.Dir == raw.DirIn && n.Type == "keepAliveResponse":
		cs.replies++
	case n.Dir == raw.DirOut && n.Type == "keepAliveRequest":
		cs.ours++
	}
}

func minTime(a, b time.Time) time.Time {
	if b.Before(a) {
		return b
	}
	return a
}

// writeConns prints each gateway's connections in order. Runs of failed
// connections that ended the same way are printed as one line.
func (c *Collector) writeConns(w io.Writer) {
	byGW := map[string][]*connStat{}
	for _, cs := range c.conns {
		byGW[cs.gateway] = append(byGW[cs.gateway], cs)
	}
	off := func(t time.Time) string { return "+" + t.Sub(c.First).Round(time.Second).String() }
	fmt.Fprintln(w, "\nConnections by gateway (times are offsets from the first message in the window)")
	for _, gw := range sortedKeys(byGW) {
		cs := byGW[gw]
		slices.SortFunc(cs, func(a, b *connStat) int { return a.first.Compare(b.first) })
		fmt.Fprintf(w, "  %s\n", gw)
		for i := 0; i < len(cs); {
			s := cs[i]
			if !s.hello && s.frames == 0 {
				j := i + 1
				for j < len(cs) && !cs[j].hello && cs[j].frames == 0 && cs[j].end == s.end {
					j++
				}
				fmt.Fprintf(w, "    %s .. %s  %d connection(s) without a hello reply, ended: %s\n",
					off(s.first), off(cs[j-1].last), j-i, orOpen(s.end))
				i = j
				continue
			}
			fmt.Fprintf(w, "    %s .. %s  %s, %d frames, gateway keep-alives %d%s, ours %d answered %d, ended: %s\n",
				off(s.first), off(s.last), s.last.Sub(s.first).Round(time.Second), s.frames,
				len(s.gwKeepAlive), gaps(s.gwKeepAlive), s.ours, s.replies, orOpen(s.end))
			i++
		}
	}
}

func orOpen(end string) string {
	if end == "" {
		return "still open"
	}
	return end
}

// gaps describes the intervals between times: median, min and max.
func gaps(ts []time.Time) string {
	if len(ts) < 2 {
		return ""
	}
	var d []time.Duration
	for i := 1; i < len(ts); i++ {
		d = append(d, ts[i].Sub(ts[i-1]))
	}
	slices.Sort(d)
	r := func(x time.Duration) time.Duration { return x.Round(time.Second) }
	return fmt.Sprintf(" every %s (%s to %s)", r(d[len(d)/2]), r(d[0]), r(d[len(d)-1]))
}
