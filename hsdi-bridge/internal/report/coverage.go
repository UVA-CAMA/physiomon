package report

import (
	_ "embed"
	"encoding/xml"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/UVA-CAMA/physiomon/hsdi-bridge/internal/hsdi"
)

//go:embed manual_paths.txt
var manualPathsText string

// ManualPaths returns the element paths the manual lists, in its order, as
// parameter/service/path.
func ManualPaths() []string {
	var out []string
	for _, l := range strings.Split(manualPathsText, "\n") {
		l = strings.TrimSpace(l)
		if l != "" && !strings.HasPrefix(l, "#") {
			out = append(out, l)
		}
	}
	return out
}

// Attributes that describe encoding rather than which signal it is. The
// coverage section lists the others, such as lead and respMethod, because a
// path alone may not say which signal a leaf carries.
var encodingAttrs = map[string]bool{
	"BT": true, "asizeBT": true, "S": true, "U": true, "offset": true,
	"INV": true, "mapINV": true, "INV_LE": true, "INV_GE": true,
	"iBV": true, "eBV": true, "token": true, "bitFieldOrder": true,
}

// Coverage records which signal paths appear in descriptors, to compare with
// the manual's lists. A signal is an element with a BT attribute below
// params/<parameter>/<service>.
type Coverage struct {
	allow Allow
	paths map[string]*coverStat
}

type coverStat struct {
	descriptors int
	sessions    map[string]bool // in memory only; only the count is printed
	types       map[string]int  // BT and asizeBT
	units       map[string]int
	attrs       map[string]map[string]int
	unlisted    map[string]bool // attribute names the manual does not list
}

// NewCoverage returns an empty Coverage that shows attribute values allowed
// by allow.
func NewCoverage(allow Allow) *Coverage {
	return &Coverage{allow: allow, paths: map[string]*coverStat{}}
}

// Add records the signal paths of one descriptor.
func (c *Coverage) Add(session string, doc []byte) error {
	seen := map[string]bool{}
	return walk(doc, func(_ int, path string, e xml.StartElement, _ bool) {
		i := strings.Index(path, "/params/")
		if i < 0 {
			return
		}
		bt := ""
		for _, a := range e.Attr {
			if a.Name.Local == "BT" {
				bt = a.Value
			}
		}
		rel := path[i+len("/params/"):]
		if bt == "" || strings.Count(rel, "/") < 2 {
			return
		}
		s := c.paths[rel]
		if s == nil {
			s = &coverStat{sessions: map[string]bool{}, types: map[string]int{}, units: map[string]int{},
				attrs: map[string]map[string]int{}, unlisted: map[string]bool{}}
			c.paths[rel] = s
		}
		if !seen[rel] {
			seen[rel] = true
			s.descriptors++
			s.sessions[session] = true
		}
		typ, unit := bt, ""
		for _, a := range e.Attr {
			n := a.Name.Local
			switch {
			case a.Name.Space == "xmlns" || n == "xmlns":
			case n == "asizeBT":
				typ += " ×" + a.Value
			case n == "U":
				unit = a.Value
			case encodingAttrs[n]:
			default:
				v := withheld
				if c.allow(path, n) {
					v = hsdi.Redact(a.Value)
				}
				if s.attrs[n] == nil {
					s.attrs[n] = map[string]int{}
				}
				s.attrs[n][v]++
				if unlistedAttrs[n] || !descriptorAttrs[n] && n != "label" {
					s.unlisted[n] = true
				}
			}
		}
		s.types[typ]++
		if unit != "" {
			s.units[unit]++
		}
	})
}

// Write prints the manual's paths, seen or not, then paths seen that the
// manual does not list.
func (c *Coverage) Write(w io.Writer) {
	manual := ManualPaths()
	inManual := map[string]bool{}
	seen := 0
	for _, p := range manual {
		inManual[p] = true
		if c.paths[p] != nil {
			seen++
		}
	}
	fmt.Fprintln(w, "Signal paths compared with the manual's lists (4-22 to 4-28)")
	fmt.Fprintln(w, "A path is parameter/service/element. The manual's sub-paths such as respRate/spontaneous may")
	fmt.Fprintln(w, "arrive as attributes (respMethod, respSite) instead; check the attributes listed.")
	fmt.Fprintf(w, "  manual paths seen: %d of %d; other paths seen: %d\n", seen, len(manual), len(c.paths)-seen)

	fmt.Fprintln(w, "\nIn the manual and seen (descriptors, sessions, types, units, other attributes)")
	for _, p := range manual {
		if s := c.paths[p]; s != nil {
			c.writePath(w, p, s)
		}
	}
	fmt.Fprintln(w, "\nIn the manual, not seen")
	for _, p := range manual {
		if c.paths[p] == nil {
			fmt.Fprintf(w, "  %s\n", p)
		}
	}
	fmt.Fprintln(w, "\nSeen, not in the manual")
	for _, p := range sortedKeys(c.paths) {
		if !inManual[p] {
			c.writePath(w, p, c.paths[p])
		}
	}
}

func (c *Coverage) writePath(w io.Writer, p string, s *coverStat) {
	fmt.Fprintf(w, "  %-60s %6d %5d  %s  %s\n", p, s.descriptors, len(s.sessions),
		topValues(s.types, 4), topValues(s.units, 4))
	names := make([]string, 0, len(s.attrs))
	for n := range s.attrs {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		note := ""
		if s.unlisted[n] {
			note = " (attribute not in the manual)"
		}
		fmt.Fprintf(w, "      @%s%s: %s\n", n, note, topValues(s.attrs[n], 12))
	}
}
