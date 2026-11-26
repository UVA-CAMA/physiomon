package xbin

import (
	"encoding/binary"
	"fmt"
	"math"
)

// Values holds one field's values from one frame. A scalar has one value; an
// array has one per element; a field inside an array of structures has the
// values of every element in turn.
type Values struct {
	Ints   []int64   // integer, boolean, NTP and UTF-8 types; unsigned 64-bit values keep their bit pattern
	Floats []float64 // xs:float and xs:double
	// Null is the null-pointer flavour, such as NULL or MISS, when the pointer
	// to this field's data, or to a structure holding it, was null (4-45).
	Null string
}

// Frame is a decoded binary frame.
type Frame struct {
	// Block header fields, zero if the descriptor has no such field.
	FormatID    uint32
	Session     [12]byte
	BlockSQN    uint32
	BlockLength int
	Start       uint32 // startDateTime, NTP seconds since 1900

	Values []Values // one per Descriptor.Fields
	// End is one past the last byte the descriptor accounts for. Bytes after
	// it are not described.
	End int
}

// maxElements bounds an array's element count read from a frame, so a
// corrupt size cannot make the decoder allocate without limit.
const maxElements = 1 << 20

// Decode decodes one frame. An error means the frame does not fit the
// descriptor: too short, a pointer outside the frame, a size too large.
func (d *Descriptor) Decode(buf []byte) (*Frame, error) {
	if len(buf) < d.FixedBytes {
		return nil, fmt.Errorf("frame is %d bytes, descriptor's fixed area %d", len(buf), d.FixedBytes)
	}
	w := walker{buf: buf, out: make([]Values, len(d.Fields))}
	pos := 0
	for _, n := range d.root {
		var err error
		if pos, err = w.node(n, pos); err != nil {
			return nil, err
		}
	}
	f := &Frame{Values: w.out, End: (w.end + 7) / 8}
	get := func(name string) []int64 {
		if i, ok := d.header[name]; ok {
			return w.out[i].Ints
		}
		return nil
	}
	if v := get("formatID"); len(v) == 1 {
		f.FormatID = uint32(v[0])
	}
	if v := get("sessionID"); len(v) == 12 {
		for i, b := range v {
			f.Session[i] = byte(b)
		}
	}
	if v := get("blockSQN"); len(v) == 1 {
		f.BlockSQN = uint32(v[0])
	}
	if v := get("blockLength"); len(v) == 1 {
		f.BlockLength = int(v[0])
	}
	if v := get("startDateTime"); len(v) == 1 {
		f.Start = uint32(v[0])
	}
	return f, nil
}

type walker struct {
	buf []byte
	out []Values
	end int // furthest bit read
}

// node reads n at bit position pos and returns the position after it in the
// area that holds it.
func (w *walker) node(n *node, pos int) (int, error) {
	if n.arr == nil {
		return w.one(n, pos)
	}
	count, at, next, null, err := w.array(n.arr, pos)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", n.where, err)
	}
	if null != "" {
		w.null(n, null)
		return next, nil
	}
	if eb := n.elemBits(); eb > 0 && count > (len(w.buf)*8-at)/eb {
		return 0, fmt.Errorf("%s: %d elements do not fit in the frame", n.where, count)
	}
	for range count {
		if at, err = w.one(n, at); err != nil {
			return 0, err
		}
	}
	if n.arr.Ptr == 0 {
		return at, nil
	}
	return next, nil
}

// one reads a single element of n.
func (w *walker) one(n *node, pos int) (int, error) {
	if n.field == nil {
		for _, k := range n.kids {
			var err error
			if pos, err = w.node(k, pos); err != nil {
				return 0, err
			}
		}
		return pos, nil
	}
	t := n.field.Type
	w.end = max(w.end, pos+t.Bits)
	switch t.Kind {
	case KindSkip, KindOpaque:
		return pos + t.Bits, nil
	}
	if t.Name[:3] == "xs:" && pos%8 != 0 {
		return 0, fmt.Errorf("%s: %s not on a byte boundary", n.where, t.Name)
	}
	v, err := w.bits(pos, t.Bits)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", n.where, err)
	}
	out := &w.out[n.idx]
	switch t.Kind {
	case KindFloat:
		if t.Bits == 32 {
			out.Floats = append(out.Floats, float64(math.Float32frombits(uint32(v))))
		} else {
			out.Floats = append(out.Floats, math.Float64frombits(v))
		}
	case KindInt:
		out.Ints = append(out.Ints, signExtend(v, t.Bits))
	case KindBool:
		if t.Bits >= 8 {
			out.Ints = append(out.Ints, signExtend(v, t.Bits))
		} else {
			out.Ints = append(out.Ints, int64(v))
		}
	default:
		out.Ints = append(out.Ints, int64(v))
	}
	return pos + t.Bits, nil
}

// array reads an array's pointer and sizes at pos. It returns the element
// count, where the elements start, where the next field starts if the array
// is behind a pointer, and the null-pointer flavour if the pointer is null.
func (w *walker) array(a *Array, pos int) (count, at, next int, null string, err error) {
	count = 1
	if a.Ptr == 0 {
		for _, d := range a.Dims {
			count *= d.N
		}
		return count, pos, 0, "", nil
	}
	if pos%8 != 0 {
		return 0, 0, 0, "", fmt.Errorf("pointer not on a byte boundary")
	}
	mul := func(n uint64) error {
		if n > maxElements || count*int(n) > maxElements {
			return fmt.Errorf("array size %d too large", uint64(count)*n)
		}
		count *= int(n)
		return nil
	}
	ptr, err := w.bits(pos, a.Ptr)
	if err != nil {
		return 0, 0, 0, "", err
	}
	pos += a.Ptr
	w.end = max(w.end, pos)
	for _, d := range a.Dims {
		switch d.Where {
		case DimFixed:
			err = mul(uint64(d.N))
		case DimHere:
			var n uint64
			if n, err = w.bits(pos, a.Ptr); err == nil {
				pos += a.Ptr
				w.end = max(w.end, pos)
				err = mul(n)
			}
		}
		if err != nil {
			return 0, 0, 0, "", err
		}
	}
	if f := nullFlavor(ptr, a.Ptr); f != "" {
		return 0, 0, pos, f, nil
	}
	at = int(ptr) * 8
	if at >= len(w.buf)*8 {
		return 0, 0, 0, "", fmt.Errorf("pointer %d is outside the %d-byte frame", ptr, len(w.buf))
	}
	for _, d := range a.Dims {
		if d.Where == DimThere {
			n, err := w.bits(at, a.Ptr)
			if err != nil {
				return 0, 0, 0, "", err
			}
			at += a.Ptr
			w.end = max(w.end, at)
			if err := mul(n); err != nil {
				return 0, 0, 0, "", err
			}
		}
	}
	return count, at, pos, "", nil
}

// null records a null pointer on every field in n.
func (w *walker) null(n *node, flavor string) {
	if n.field != nil {
		w.out[n.idx].Null = flavor
		return
	}
	for _, k := range n.kids {
		w.null(k, flavor)
	}
}

// bits reads n bits, most significant first, at bit position pos.
func (w *walker) bits(pos, n int) (uint64, error) {
	if pos < 0 || pos+n > len(w.buf)*8 {
		return 0, fmt.Errorf("read past the end of the %d-byte frame", len(w.buf))
	}
	if pos%8 == 0 {
		b := w.buf[pos/8:]
		switch n {
		case 8:
			return uint64(b[0]), nil
		case 16:
			return uint64(binary.BigEndian.Uint16(b)), nil
		case 32:
			return uint64(binary.BigEndian.Uint32(b)), nil
		case 64:
			return binary.BigEndian.Uint64(b), nil
		}
	}
	var v uint64
	for i := range n {
		p := pos + i
		v = v<<1 | uint64(w.buf[p/8]>>(7-p%8)&1)
	}
	return v, nil
}

func signExtend(v uint64, bits int) int64 {
	shift := 64 - bits
	return int64(v<<shift) >> shift
}
