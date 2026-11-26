package report

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/UVA-CAMA/physiomon/hsdi-bridge/internal/hsdi"
	"github.com/UVA-CAMA/physiomon/hsdi-bridge/internal/raw"
)

// Collector aggregates raw stream messages into a structure report. It keeps
// session IDs only in memory, to join frames to descriptors, and never prints
// them.
type Collector struct {
	Labels bool // show parameter labels in descriptors

	Messages  int
	First     time.Time
	Last      time.Time
	byKind    map[string]int // dir/kind/root
	sizes     map[string]*sizeStat
	discarded int
	events    map[string]int
	reasons   map[string]int
	binEnds   map[string]int
	parseErrs int
	conns     map[string]*connStat // connection ID -> history

	descShapes map[string]*shape // hash -> shape
	shapeOrder []string
	coverage   *Coverage
	// Descriptors read from the state stream. When there are any, shapes and
	// coverage come from them alone, not from descriptors in the raw window.
	stateDescs int
	summaries  map[string]*PathSummary // root -> summary, for non-descriptor XML

	// formatKey(session, formatID) -> channel, from descriptors
	formats    map[string]int
	frames     map[string]int // channel -> frames
	unknownFmt int
	badTopBits int
	short      int
	frameLens  map[string]map[int]int // channel -> length -> count
	streams    map[string]*streamStat // session/channel -> stats
	sessions   map[int]map[string]bool
}

type sizeStat struct {
	n, min, max int
	sum         int64
}

type shape struct {
	tree  string
	count int
	chn   map[int]int
}

type streamStat struct {
	first, last time.Time
	n           int
}

// NewCollector returns an empty Collector.
func NewCollector(labels bool) *Collector {
	return &Collector{
		Labels:     labels,
		byKind:     map[string]int{},
		sizes:      map[string]*sizeStat{},
		events:     map[string]int{},
		conns:      map[string]*connStat{},
		reasons:    map[string]int{},
		binEnds:    map[string]int{},
		descShapes: map[string]*shape{},
		summaries:  map[string]*PathSummary{},
		coverage:   NewCoverage(DescriptorAllow(labels)),
		formats:    map[string]int{},
		frames:     map[string]int{},
		frameLens:  map[string]map[int]int{},
		streams:    map[string]*streamStat{},
		sessions:   map[int]map[string]bool{},
	}
}

func formatKey(session string, formatID uint32) string {
	return session + "/" + strconv.FormatUint(uint64(formatID&0x3FFFFFFF), 10)
}

// Add takes one raw stream message.
func (c *Collector) Add(subject string, h nats.Header, data []byte) {
	c.Messages++
	t, _ := time.Parse(raw.TimeFormat, h.Get(raw.HReceived))
	if !t.IsZero() {
		if c.First.IsZero() || t.Before(c.First) {
			c.First = t
		}
		if t.After(c.Last) {
			c.Last = t
		}
	}
	n, ok := raw.ParseSubject(subject)
	if !ok {
		n = raw.Name{Dir: "?", Type: subject}
	}
	c.addConn(n, h, t, data)
	key := n.Dir + " " + n.Type
	c.byKind[key]++
	ss := c.sizes[key]
	if ss == nil {
		ss = &sizeStat{min: len(data)}
		c.sizes[key] = ss
	}
	ss.n++
	ss.sum += int64(len(data))
	ss.min = min(ss.min, len(data))
	ss.max = max(ss.max, len(data))

	switch {
	case n.Dir == raw.DirEvent:
		var e raw.Event
		json.Unmarshal(data, &e)
		c.events[n.Type]++
		if e.Reason != "" {
			c.reasons[n.Type+": "+e.Reason]++
		}
	case n.Dir == raw.DirOut:
		// Our own messages; counted above only.
	case n.Type == raw.TypeDiscarded:
		d, _ := strconv.Atoi(h.Get(raw.HDiscarded))
		c.discarded += d
	case n.Type == raw.TypeBinary:
		c.addFrame(t, data)
	default:
		c.addXML(data)
	}
}

// AddState takes one descriptor from the state stream. Call it for all of them
// before the first Add.
func (c *Collector) AddState(data []byte) {
	hd, err := hsdi.ParseHeader(data)
	if err != nil || hd.Root != "binDescriptor" {
		c.parseErrs++
		return
	}
	c.stateDescs++
	c.addDescriptor(hd, data)
}

func (c *Collector) addXML(data []byte) {
	hd, err := hsdi.ParseHeader(data)
	if err != nil {
		c.parseErrs++
		return
	}
	switch hd.Root {
	case "binDescriptor":
		if c.stateDescs > 0 {
			// Already counted from the state stream. Still map the format, in
			// case the state stream missed it.
			if id, ok := descriptorFormatID(data); ok {
				c.formats[formatKey(hd.SessionID, id)] = hd.Chn
			}
			return
		}
		c.addDescriptor(hd, data)
	default:
		if hd.Root == "binEnd" {
			c.binEnds[hsdi.Redact(hd.Message)]++
		}
		s := c.summaries[hd.Root]
		if s == nil {
			s = NewPathSummary()
			c.summaries[hd.Root] = s
		}
		if err := s.Add(data, AllowFor(hd.Root, c.Labels)); err != nil {
			c.parseErrs++
		}
	}
}

func (c *Collector) addDescriptor(hd hsdi.Header, data []byte) {
	tree, err := Tree(data, DescriptorAllow(c.Labels))
	if err != nil {
		c.parseErrs++
		return
	}
	sum := sha256.Sum256([]byte(tree))
	k := hex.EncodeToString(sum[:8])
	s := c.descShapes[k]
	if s == nil {
		s = &shape{tree: tree, chn: map[int]int{}}
		c.descShapes[k] = s
		c.shapeOrder = append(c.shapeOrder, k)
	}
	s.count++
	s.chn[hd.Chn]++
	c.coverage.Add(hd.SessionID, data)
	if id, ok := descriptorFormatID(data); ok {
		c.formats[formatKey(hd.SessionID, id)] = hd.Chn
	}
	if c.sessions[hd.Chn] == nil {
		c.sessions[hd.Chn] = map[string]bool{}
	}
	c.sessions[hd.Chn][hd.SessionID] = true
}

// descriptorFormatID reads the iBV of block/formatID.
func descriptorFormatID(data []byte) (uint32, bool) {
	var id uint32
	found := false
	walk(data, func(_ int, path string, e xml.StartElement, _ bool) {
		if found || !strings.HasSuffix(path, "/block/formatID") {
			return
		}
		for _, a := range e.Attr {
			if a.Name.Local == "iBV" {
				if v, err := strconv.ParseUint(a.Value, 10, 32); err == nil {
					id, found = uint32(v), true
				}
			}
		}
	})
	return id, found
}

func (c *Collector) addFrame(t time.Time, data []byte) {
	if len(data) < 16 {
		c.short++
		return
	}
	fid := binary.BigEndian.Uint32(data)
	if fid>>30 != 1 {
		c.badTopBits++
	}
	sess := strings.ToUpper(hex.EncodeToString(data[4:16]))
	chn, ok := c.formats[formatKey(sess, fid)]
	ch := "unknown"
	if !ok {
		c.unknownFmt++
	} else {
		ch = strconv.Itoa(chn)
	}
	c.frames[ch]++
	if c.frameLens[ch] == nil {
		c.frameLens[ch] = map[int]int{}
	}
	c.frameLens[ch][len(data)]++
	sk := sess + "/" + ch
	st := c.streams[sk]
	if st == nil {
		st = &streamStat{first: t}
		c.streams[sk] = st
	}
	st.n++
	st.last = t
}

// Write prints the report.
func (c *Collector) Write(w io.Writer) {
	fmt.Fprintln(w, "HSDI raw stream structure report")
	fmt.Fprintln(w, "Values are withheld (…) unless they describe signals or protocol. No session IDs, names,")
	fmt.Fprintln(w, "identifiers, locations or gateway dates are printed. Review before sharing.")
	fmt.Fprintln(w)
	span := c.Last.Sub(c.First)
	fmt.Fprintf(w, "Messages: %d over %s\n\n", c.Messages, span.Round(time.Second))

	fmt.Fprintln(w, "Messages by direction and type (count, bytes min / mean / max)")
	for _, k := range sortedKeys(c.byKind) {
		s := c.sizes[k]
		fmt.Fprintf(w, "  %-45s %9d   %7d / %7d / %7d\n", k, c.byKind[k], s.min, s.sum/int64(s.n), s.max)
	}

	fmt.Fprintf(w, "Bytes discarded while resynchronising: %d (hsdi-report -discarded prints them)\n", c.discarded)
	fmt.Fprintf(w, "XML that could not be parsed: %d\n", c.parseErrs)

	fmt.Fprintln(w, "\nBridge events")
	for _, k := range sortedKeys(c.events) {
		fmt.Fprintf(w, "  %-20s %d\n", k, c.events[k])
	}
	for _, k := range sortedKeys(c.reasons) {
		fmt.Fprintf(w, "    %s ×%d\n", k, c.reasons[k])
	}
	c.writeConns(w)
	if len(c.binEnds) > 0 {
		fmt.Fprintln(w, "binEnd reasons")
		for _, k := range sortedKeys(c.binEnds) {
			fmt.Fprintf(w, "  %q ×%d\n", k, c.binEnds[k])
		}
	}

	fmt.Fprintln(w, "\nBinary frames by channel (from the descriptor with the same session and format ID)")
	for _, ch := range sortedKeys(c.frames) {
		fmt.Fprintf(w, "  channel %s: %d frames\n", ch, c.frames[ch])
		fmt.Fprintf(w, "    lengths: %s\n", topInts(c.frameLens[ch], 12))
		var rates []float64
		for k, st := range c.streams {
			if strings.HasSuffix(k, "/"+ch) && st.n > 1 {
				if d := st.last.Sub(st.first).Seconds(); d > 0 {
					rates = append(rates, float64(st.n-1)/d)
				}
			}
		}
		if len(rates) > 0 {
			sort.Float64s(rates)
			fmt.Fprintf(w, "    streams: %d; frames per second per stream min %.2f, median %.2f, max %.2f\n",
				len(rates), rates[0], rates[len(rates)/2], rates[len(rates)-1])
		}
	}
	fmt.Fprintf(w, "  frames with no matching descriptor: %d\n", c.unknownFmt)
	fmt.Fprintf(w, "  frames whose format ID does not start with bits 01: %d\n", c.badTopBits)
	fmt.Fprintf(w, "  frames shorter than 16 bytes: %d\n", c.short)
	for _, ch := range sortedIntKeys(map[int]int(countSessions(c.sessions))) {
		fmt.Fprintf(w, "  sessions with a descriptor on channel %d: %d\n", ch, len(c.sessions[ch]))
	}

	if c.stateDescs > 0 {
		fmt.Fprintf(w, "\nDescriptors below: all %d in the state stream, whatever the raw window.\n", c.stateDescs)
	} else {
		fmt.Fprintln(w, "\nDescriptors below: only those in the raw window. Frames of sessions whose descriptor came")
		fmt.Fprintln(w, "before the window show as having no matching descriptor.")
	}
	fmt.Fprintf(w, "Descriptor shapes: %d distinct\n", len(c.shapeOrder))
	order := append([]string(nil), c.shapeOrder...)
	sort.SliceStable(order, func(i, j int) bool { return c.descShapes[order[i]].count > c.descShapes[order[j]].count })
	for i, k := range order {
		s := c.descShapes[k]
		var chs []string
		for _, ch := range sortedIntKeys(s.chn) {
			chs = append(chs, fmt.Sprintf("channel %d ×%d", ch, s.chn[ch]))
		}
		fmt.Fprintf(w, "\n--- shape %d: %d descriptors (%s)\n", i+1, s.count, strings.Join(chs, ", "))
		io.WriteString(w, s.tree)
	}

	fmt.Fprintln(w)
	c.coverage.Write(w)

	for _, root := range sortedKeys(c.summaries) {
		s := c.summaries[root]
		fmt.Fprintf(w, "\n=== %s: %d messages; element paths, occurrences, attribute values\n", root, s.Docs)
		s.Write(w, 12)
	}
}

func countSessions(m map[int]map[string]bool) map[int]int {
	out := map[int]int{}
	for k, v := range m {
		out[k] = len(v)
	}
	return out
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func sortedIntKeys(m map[int]int) []int {
	keys := make([]int, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	return keys
}

func topInts(m map[int]int, n int) string {
	type kv struct{ k, v int }
	var all []kv
	for k, v := range m {
		all = append(all, kv{k, v})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].v > all[j].v || (all[i].v == all[j].v && all[i].k < all[j].k) })
	var parts []string
	for i, e := range all {
		if i == n {
			parts = append(parts, fmt.Sprintf("(%d more lengths)", len(all)-n))
			break
		}
		parts = append(parts, fmt.Sprintf("%d B ×%d", e.k, e.v))
	}
	return strings.Join(parts, ", ")
}
