// Package report summarises the structure of HSDI XML without its values, so
// that a summary of real gateway traffic can be reviewed and shared without
// patient data.
//
// Every attribute value and every piece of text is withheld unless an allow
// rule names it. Withheld values print as "…". Shown values are passed
// through hsdi.Redact, because the gateway puts session IDs in free text. Message <header> elements are
// skipped entirely, because they hold session IDs and dates.
package report

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/UVA-CAMA/physiomon/hsdi-bridge/internal/hsdi"
)

// Allow decides whether an attribute's value may be shown. path is the
// element path from the message element, such as
// "binDescriptor/block/groupBT/params/ecg".
type Allow func(path, attr string) bool

// Attributes whose values describe signals and layout, not patients (manual
// 4-2 to 4-6 and SUMMARY section 11). Initial values (iBV) are withheld
// because they differ between otherwise identical descriptors.
var descriptorAttrs = map[string]bool{
	"BT": true, "asizeBT": true, "S": true, "U": true, "offset": true,
	"INV": true, "mapINV": true, "INV_LE": true, "INV_GE": true,
	"source": true, "index": true, "unit": true, "site": true, "lead": true,
	"gas": true, "aaGas": true, "respGas": true, "parameterPresent": true,
	"rate": true, "interval": true, "repeat": true, "type": true,
	"token": true, "eBV": true, "bitFieldOrder": true,
}

// Attributes the manual does not list but a real gateway sends. Their values
// name methods and sites, not patients.
var unlistedAttrs = map[string]bool{
	"respMethod": true, "respSite": true, "averageValue": true,
}

// DescriptorAllow shows the signal metadata attributes. Labels are shown only
// if labels is set: the manual says they are usually set by the user (4-4).
func DescriptorAllow(labels bool) Allow {
	return func(_, attr string) bool {
		return descriptorAttrs[attr] || unlistedAttrs[attr] || (labels && attr == "label")
	}
}

// Elements of a session update whose V attribute is a fixed vocabulary
// (4-12 to 4-17). Names, identifiers, dates, ages, locations are withheld.
var sessionValueElems = map[string]bool{
	"service": true, "network": true, "patientStatus": true, "modelID": true,
	"vendorID": true, "manufacturerName": true, "unknownID": true, "primary": true,
}

// SessionUpdateAllow shows status and type attributes and the V of
// fixed-vocabulary elements.
func SessionUpdateAllow(path, attr string) bool {
	if attr == "status" || attr == "type" {
		return true
	}
	if attr == "V" {
		return sessionValueElems[path[strings.LastIndexByte(path, '/')+1:]]
	}
	return false
}

// OtherAllow is for the remaining message types: statuses, the binEnd reason,
// and a few protocol values.
func OtherAllow(path, attr string) bool {
	switch attr {
	case "status", "message", "more":
		return true
	case "V", "U":
		last := path[strings.LastIndexByte(path, '/')+1:]
		return last == "newMsgCHN" || last == "byteOrder" || last == "accuracy"
	}
	return false
}

// AllowFor returns the rule for a message type.
func AllowFor(root string, labels bool) Allow {
	switch root {
	case "binDescriptor":
		return DescriptorAllow(labels)
	case "sessionUpdate":
		return SessionUpdateAllow
	}
	return OtherAllow
}

const withheld = "…"

// Tree returns the element tree of doc, one element per line, indented, with
// attributes in document order. Use it where order matters, as in descriptors.
func Tree(doc []byte, allow Allow) (string, error) {
	var b strings.Builder
	err := walk(doc, func(depth int, path string, e xml.StartElement, hasText bool) {
		b.WriteString(strings.Repeat("  ", depth))
		b.WriteString(e.Name.Local)
		for _, a := range e.Attr {
			if a.Name.Space == "xmlns" || a.Name.Local == "xmlns" {
				continue
			}
			v := withheld
			if allow(path, a.Name.Local) {
				v = hsdi.Redact(a.Value)
			}
			fmt.Fprintf(&b, " %s=%q", a.Name.Local, v)
		}
		if hasText {
			b.WriteString(" [text " + withheld + "]")
		}
		b.WriteByte('\n')
	})
	return b.String(), err
}

// PathSummary aggregates element paths over many documents: how often each
// path occurs, and for each attribute the values seen (or "…").
type PathSummary struct {
	Docs  int
	Paths map[string]*PathStat
}

// PathStat is one element path's counts.
type PathStat struct {
	Count int
	Text  int
	Attrs map[string]map[string]int
}

// NewPathSummary returns an empty summary.
func NewPathSummary() *PathSummary { return &PathSummary{Paths: map[string]*PathStat{}} }

// Add adds one document.
func (s *PathSummary) Add(doc []byte, allow Allow) error {
	s.Docs++
	return walk(doc, func(_ int, path string, e xml.StartElement, hasText bool) {
		ps := s.Paths[path]
		if ps == nil {
			ps = &PathStat{Attrs: map[string]map[string]int{}}
			s.Paths[path] = ps
		}
		ps.Count++
		if hasText {
			ps.Text++
		}
		for _, a := range e.Attr {
			if a.Name.Space == "xmlns" || a.Name.Local == "xmlns" {
				continue
			}
			v := withheld
			if allow(path, a.Name.Local) {
				v = hsdi.Redact(a.Value)
			}
			if ps.Attrs[a.Name.Local] == nil {
				ps.Attrs[a.Name.Local] = map[string]int{}
			}
			ps.Attrs[a.Name.Local][v]++
		}
	})
}

// Write prints the summary, paths sorted. At most maxValues values are listed
// per attribute.
func (s *PathSummary) Write(w io.Writer, maxValues int) {
	paths := make([]string, 0, len(s.Paths))
	for p := range s.Paths {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, p := range paths {
		ps := s.Paths[p]
		fmt.Fprintf(w, "  %-70s %8d", p, ps.Count)
		if ps.Text > 0 {
			fmt.Fprintf(w, "  text %d", ps.Text)
		}
		fmt.Fprintln(w)
		names := make([]string, 0, len(ps.Attrs))
		for n := range ps.Attrs {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, n := range names {
			fmt.Fprintf(w, "      @%s: %s\n", n, topValues(ps.Attrs[n], maxValues))
		}
	}
}

func topValues(m map[string]int, n int) string {
	type kv struct {
		k string
		v int
	}
	var all []kv
	for k, v := range m {
		all = append(all, kv{k, v})
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].v != all[j].v {
			return all[i].v > all[j].v
		}
		return all[i].k < all[j].k
	})
	var parts []string
	for i, e := range all {
		if i == n {
			parts = append(parts, fmt.Sprintf("(%d more values)", len(all)-n))
			break
		}
		parts = append(parts, fmt.Sprintf("%q×%d", e.k, e.v))
	}
	return strings.Join(parts, " ")
}

// walk calls f for each element below the <sapphire> root, skipping <header>
// subtrees. path starts at the message element. hasText reports non-blank
// character data directly inside the element; it is computed before f is
// called, so walk reads each element's content first.
func walk(doc []byte, f func(depth int, path string, e xml.StartElement, hasText bool)) error {
	d := xml.NewDecoder(bytes.NewReader(doc))
	type frame struct {
		e       xml.StartElement
		path    string
		text    bool
		emitted bool
	}
	var stack []*frame
	skip := 0
	emit := func(fr *frame, depth int) {
		if !fr.emitted {
			fr.emitted = true
			f(depth, fr.path, fr.e, fr.text)
		}
	}
	for {
		tok, err := d.Token()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		switch t := tok.(type) {
		case xml.StartElement:
			if skip > 0 || (len(stack) == 2 && t.Name.Local == "header") {
				skip++
				continue
			}
			if len(stack) > 1 {
				// The parent's text, if any, came before this child.
				emit(stack[len(stack)-1], len(stack)-2)
			}
			path := t.Name.Local
			if len(stack) > 1 {
				path = stack[len(stack)-1].path + "/" + t.Name.Local
			}
			stack = append(stack, &frame{e: t.Copy(), path: path})
		case xml.EndElement:
			if skip > 0 {
				skip--
				continue
			}
			if len(stack) == 0 {
				continue
			}
			if len(stack) > 1 {
				emit(stack[len(stack)-1], len(stack)-2)
			}
			stack = stack[:len(stack)-1]
		case xml.CharData:
			if skip == 0 && len(stack) > 1 && len(bytes.TrimSpace(t)) > 0 {
				stack[len(stack)-1].text = true
			}
		}
	}
}
