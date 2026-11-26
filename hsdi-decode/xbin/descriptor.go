// Package xbin reads XML-binary descriptors (<binDescriptor>) and decodes the
// binary frames they describe (manual 4-20 to 4-46).
//
// It knows no signals. Every name, unit, scale factor and invalid value comes
// from the descriptor, so a signal never seen before decodes like any other.
// Every binary type in the manual has a known size, so elements this package
// cannot decode are skipped without misaligning the rest of the frame (the
// "Must Ignore" rule, 5-2). A descriptor whose layout cannot be worked out is
// rejected whole.
//
// Frames are big-endian, the only order the gateway sends (4-8). BPTR
// pointers count from the first byte of the frame, as the manual's example on
// 4-31 requires and real frames confirm.
package xbin

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Field is one element with a BT attribute: a block header field such as
// blockSQN, or a signal such as params/ip/num/systolic.
type Field struct {
	Path  string            // element path below <block>, such as groupBT/params/ip/num/systolic
	Name  string            // element name, such as systolic
	Type  Type              //
	Array *Array            // nil for a scalar
	Attrs map[string]string // all the element's attributes

	// Set for signals, the fields below params/<parameter>/<service>.
	Param      string            // such as ip
	Service    string            // wav, num or cfg
	ParamAttrs map[string]string // the parameter element's attributes: source, index, label, site, ...
	// Repeated is set inside an array of structures, such as an episodic
	// array: the field has a value, or an array of values, per element.
	Repeated bool

	Unit          string           // U
	Scale         float64          // S, default 1
	Offset        float64          // offset, default 0
	OffsetUnknown bool             // offset="UNK"
	Enum          map[int64]string // eBV to token, from <enumList>

	inv          []invalid
	invLE, invGE *invalid
}

type invalid struct {
	i      int64
	f      float64
	reason string
}

// SignalPath returns parameter/service/element..., such as
// ip/num/pawMeasurements/episodic/paw, or "" for a header field.
func (f *Field) SignalPath() string {
	if f.Param == "" {
		return ""
	}
	_, rest, _ := strings.Cut(f.Path, "params/")
	return rest
}

// Invalid reports whether raw integer value v is invalid, and why: the
// mapINV token for an INV value, or "INV" (4-5, 4-6).
func (f *Field) Invalid(v int64) (string, bool) {
	for _, x := range f.inv {
		if v == x.i {
			return x.reason, true
		}
	}
	if f.invLE != nil && v <= f.invLE.i {
		return "INV", true
	}
	if f.invGE != nil && v >= f.invGE.i {
		return "INV", true
	}
	return "", false
}

// InvalidList returns the invalid values (INV) with their reasons (mapINV,
// or "INV"), and the INV_LE and INV_GE bounds, nil if absent. Integer values
// only.
func (f *Field) InvalidList() (values []int64, reasons []string, le, ge *int64) {
	for _, x := range f.inv {
		values, reasons = append(values, x.i), append(reasons, x.reason)
	}
	if f.invLE != nil {
		le = &f.invLE.i
	}
	if f.invGE != nil {
		ge = &f.invGE.i
	}
	return values, reasons, le, ge
}

// InvalidFloat is Invalid for float types.
func (f *Field) InvalidFloat(v float64) (string, bool) {
	for _, x := range f.inv {
		if v == x.f {
			return x.reason, true
		}
	}
	if f.invLE != nil && v <= f.invLE.f {
		return "INV", true
	}
	if f.invGE != nil && v >= f.invGE.f {
		return "INV", true
	}
	return "", false
}

// Descriptor is a parsed <binDescriptor>.
type Descriptor struct {
	SessionID  string            // from the message header, upper case hex
	FormatID   uint32            // the formatID field's iBV, 0 if none
	BlockAttrs map[string]string // <block> attributes: rate or interval, U, repeat
	Fields     []*Field          // in document order: header fields, then signals
	FixedBytes int               // size of the fixed area of a frame, header included

	root   []*node
	header map[string]int // header field name to index in Fields
}

// node is one element of the layout: a field, or a structure of nodes,
// either of which may be an array.
type node struct {
	where string // element path below <block>
	field *Field
	idx   int // index of field in Descriptor.Fields
	arr   *Array
	kids  []*node
}

// elemBits is the fixed-area size of one element.
func (n *node) elemBits() int {
	if n.field != nil {
		return n.field.Type.Bits
	}
	b := 0
	for _, k := range n.kids {
		b += k.bits()
	}
	return b
}

// bits is the node's size in the area that holds it.
func (n *node) bits() int {
	if n.arr == nil {
		return n.elemBits()
	}
	return n.arr.fixedBits(n.elemBits())
}

type elem struct {
	name  string
	attrs map[string]string
	kids  []*elem
}

// Parse reads a <binDescriptor> message.
func Parse(doc []byte) (*Descriptor, error) {
	root, session, err := readTree(doc)
	if err != nil {
		return nil, err
	}
	bd := find(root, "binDescriptor")
	if bd == nil {
		return nil, errors.New("no binDescriptor element")
	}
	block := find(bd, "block")
	if block == nil {
		return nil, errors.New("no block element")
	}
	d := &Descriptor{SessionID: strings.ToUpper(session), BlockAttrs: block.attrs, header: map[string]int{}}
	for _, k := range block.kids {
		n, err := d.compile(k, []*elem{k}, false)
		if err != nil {
			return nil, err
		}
		if n != nil {
			d.root = append(d.root, n)
		}
	}
	bits := 0
	for _, n := range d.root {
		bits += n.bits()
	}
	if bits%8 != 0 {
		return nil, fmt.Errorf("fixed area is %d bits, not whole bytes", bits)
	}
	d.FixedBytes = bits / 8
	for i, f := range d.Fields {
		if !strings.Contains(f.Path, "/") {
			d.header[f.Name] = i
		}
	}
	if i, ok := d.header["formatID"]; ok {
		if v, err := strconv.ParseUint(d.Fields[i].Attrs["iBV"], 10, 32); err == nil {
			d.FormatID = uint32(v)
		}
	}
	return d, nil
}

// readTree reads the document into elements, by local name, without the
// message <header>, from which it returns the session ID.
func readTree(doc []byte) (root *elem, session string, err error) {
	dec := xml.NewDecoder(bytes.NewReader(doc))
	top := &elem{}
	stack := []*elem{top}
	skip := 0
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			return top, session, nil
		}
		if err != nil {
			return nil, "", err
		}
		switch t := tok.(type) {
		case xml.StartElement:
			if skip > 0 || (t.Name.Local == "header" && stack[len(stack)-1].name == "binDescriptor") {
				if skip == 1 && t.Name.Local == "sessionID" {
					for _, a := range t.Attr {
						if a.Name.Local == "V" {
							session = a.Value
						}
					}
				}
				skip++
				continue
			}
			e := &elem{name: t.Name.Local, attrs: map[string]string{}}
			for _, a := range t.Attr {
				if a.Name.Space != "xmlns" && a.Name.Local != "xmlns" {
					e.attrs[a.Name.Local] = a.Value
				}
			}
			parent := stack[len(stack)-1]
			parent.kids = append(parent.kids, e)
			stack = append(stack, e)
		case xml.EndElement:
			if skip > 0 {
				skip--
				continue
			}
			stack = stack[:len(stack)-1]
		}
	}
}

func find(e *elem, name string) *elem {
	for _, k := range e.kids {
		if k.name == name {
			return k
		}
		if f := find(k, name); f != nil {
			return f
		}
	}
	return nil
}

// compile turns an element into a layout node, or nil if it occupies no
// bytes. path holds the element and its ancestors below <block>.
func (d *Descriptor) compile(e *elem, path []*elem, repeated bool) (*node, error) {
	names := make([]string, len(path))
	for i, p := range path {
		names[i] = p.name
	}
	where := strings.Join(names, "/")
	bt, hasBT := e.attrs["BT"]
	if e.attrs["bitFieldOrder"] == "littleEndian" {
		return nil, fmt.Errorf("%s: little-endian bit fields are not supported", where)
	}
	if _, hasV := e.attrs["V"]; hasV && !hasBT {
		return nil, nil // a value carried in the XML
	}
	var arr *Array
	if s, ok := e.attrs["asizeBT"]; ok {
		a, err := ParseArray(s)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", where, err)
		}
		arr = a
	}
	if r, ok := e.attrs["repeat"]; ok && e.name == "groupBT" && r != "1" {
		n, err := strconv.Atoi(r)
		if err != nil || n < 0 {
			return nil, fmt.Errorf("%s: repeat %q", where, r)
		}
		arr = &Array{Dims: []Dim{{N: n}}}
	}

	if hasBT {
		t, err := ParseType(bt)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", where, err)
		}
		f, err := newField(e, path, where, t, arr, repeated)
		if err != nil {
			return nil, err
		}
		d.Fields = append(d.Fields, f)
		n := &node{where: where, field: f, idx: len(d.Fields) - 1, arr: arr}
		// An element with BT may also hold elements with BT, such as
		// respRate with respRate/spontaneous. Their values follow the
		// element's own, in document order.
		var kids []*node
		for _, k := range e.kids {
			if k.name == "enumList" {
				continue
			}
			kn, err := d.compile(k, append(path[:len(path):len(path)], k), repeated)
			if err != nil {
				return nil, err
			}
			if kn != nil {
				kids = append(kids, kn)
			}
		}
		if len(kids) == 0 {
			return n, nil
		}
		if arr != nil {
			return nil, fmt.Errorf("%s: an array with elements inside it is not supported", where)
		}
		return &node{where: where, kids: append([]*node{n}, kids...)}, nil
	}

	n := &node{where: where, arr: arr}
	for _, k := range e.kids {
		if k.name == "enumList" {
			continue
		}
		kn, err := d.compile(k, append(path[:len(path):len(path)], k), repeated || arr != nil)
		if err != nil {
			return nil, err
		}
		if kn != nil {
			n.kids = append(n.kids, kn)
		}
	}
	if len(n.kids) == 0 {
		return nil, nil
	}
	return n, nil
}

func newField(e *elem, path []*elem, where string, t Type, arr *Array, repeated bool) (*Field, error) {
	f := &Field{Path: where, Name: e.name, Type: t, Array: arr, Attrs: e.attrs, Repeated: repeated,
		Unit: e.attrs["U"], Scale: 1}
	for i, p := range path {
		if p.name == "params" && i+3 < len(path) {
			f.Param, f.Service, f.ParamAttrs = path[i+1].name, path[i+2].name, path[i+1].attrs
			break
		}
	}
	var err error
	if s, ok := e.attrs["S"]; ok {
		if f.Scale, err = parseScale(s); err != nil {
			return nil, fmt.Errorf("%s: S=%q: %w", where, s, err)
		}
	}
	if s, ok := e.attrs["offset"]; ok {
		if s == "UNK" {
			f.OffsetUnknown = true
		} else if f.Offset, err = strconv.ParseFloat(s, 64); err != nil {
			return nil, fmt.Errorf("%s: offset=%q", where, s)
		}
	}
	reasons := strings.Fields(e.attrs["mapINV"])
	for i, s := range strings.Fields(e.attrs["INV"]) {
		x, err := parseInvalid(s)
		if err != nil {
			return nil, fmt.Errorf("%s: INV: %w", where, err)
		}
		x.reason = "INV"
		if i < len(reasons) {
			x.reason = reasons[i]
		}
		f.inv = append(f.inv, x)
	}
	for _, b := range []struct {
		attr string
		dst  **invalid
	}{{"INV_LE", &f.invLE}, {"INV_GE", &f.invGE}} {
		if s, ok := e.attrs[b.attr]; ok {
			x, err := parseInvalid(s)
			if err != nil {
				return nil, fmt.Errorf("%s: %s: %w", where, b.attr, err)
			}
			*b.dst = &x
		}
	}
	for _, k := range e.kids {
		if k.name != "enumList" {
			continue
		}
		for _, en := range k.kids {
			v, err := strconv.ParseInt(en.attrs["eBV"], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("%s: enum eBV %q", where, en.attrs["eBV"])
			}
			if f.Enum == nil {
				f.Enum = map[int64]string{}
			}
			f.Enum[v] = en.attrs["token"]
		}
	}
	return f, nil
}

// parseScale reads S: a decimal, or a ratio such as 10/1024 (4-2).
func parseScale(s string) (float64, error) {
	if n, d, ok := strings.Cut(s, "/"); ok {
		num, err1 := strconv.ParseFloat(n, 64)
		den, err2 := strconv.ParseFloat(d, 64)
		if err1 != nil || err2 != nil || den == 0 {
			return 0, errors.New("bad ratio")
		}
		return num / den, nil
	}
	return strconv.ParseFloat(s, 64)
}

// parseInvalid reads one invalid value. Unsigned 64-bit values above the
// int64 range keep their bit pattern, as decoded values do.
func parseInvalid(s string) (invalid, error) {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return invalid{}, fmt.Errorf("%q is not a number", s)
	}
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		u, uerr := strconv.ParseUint(s, 10, 64)
		if uerr != nil {
			i = int64(f)
		} else {
			i = int64(u)
		}
	}
	return invalid{i: i, f: f}, nil
}
