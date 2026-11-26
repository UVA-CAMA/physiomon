package xbin

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Kind says how a binary type's bits become a value.
type Kind int

const (
	KindInt    Kind = iota // signed integer
	KindUint               // unsigned integer
	KindFloat              // IEEE 754, 32 or 64 bits
	KindBool               // 1 true, 0 false; 8 and 16 bit ones are read as signed, so INV="-128" matches
	KindNTP                // NTP-32: seconds since 1900; NTP-64: seconds in the upper 32 bits, fraction in the lower
	KindUTF8               // one byte of a UTF-8 string
	KindSkip               // xb:null, xb:void, xb:pad: occupies bits, has no value
	KindOpaque             // size known, meaning not: unsigned-128, and xb: types this package does not know
)

// Type is a binary type from a BT attribute (manual 4-36, 4-37).
type Type struct {
	Name string
	Bits int
	Kind Kind
}

var namedTypes = map[string]Type{
	"xs:byte":          {Bits: 8, Kind: KindInt},
	"xs:unsignedByte":  {Bits: 8, Kind: KindUint},
	"xs:short":         {Bits: 16, Kind: KindInt},
	"xs:unsignedShort": {Bits: 16, Kind: KindUint},
	"xs:int":           {Bits: 32, Kind: KindInt},
	"xs:unsignedInt":   {Bits: 32, Kind: KindUint},
	"xs:long":          {Bits: 64, Kind: KindInt},
	"xs:unsignedLong":  {Bits: 64, Kind: KindUint},
	"xs:float":         {Bits: 32, Kind: KindFloat},
	"xs:double":        {Bits: 64, Kind: KindFloat},
	"xs:boolean":       {Bits: 8, Kind: KindBool},
	"xb:UTF-8":         {Bits: 8, Kind: KindUTF8},
	"xb:NTP-64":        {Bits: 64, Kind: KindNTP},
	"xb:NTP-32":        {Bits: 32, Kind: KindNTP},
	// The manual's table says xs:, its list of names xb:.
	"xs:unsigned-128": {Bits: 128, Kind: KindOpaque},
	"xb:unsigned-128": {Bits: 128, Kind: KindOpaque},
}

var sizedType = regexp.MustCompile(`^xb:([A-Za-z][A-Za-z0-9]*)-([0-9]+)$`)

// ParseType reads a BT attribute. Any xb: type with a -nn suffix has a known
// size, even if its name is new (4-37); one this package does not know is
// KindOpaque.
func ParseType(bt string) (Type, error) {
	if t, ok := namedTypes[bt]; ok {
		t.Name = bt
		return t, nil
	}
	m := sizedType.FindStringSubmatch(bt)
	if m == nil {
		return Type{}, fmt.Errorf("unknown binary type %q", bt)
	}
	n, _ := strconv.Atoi(m[2])
	t := Type{Name: bt, Bits: n, Kind: KindOpaque}
	switch m[1] {
	case "signed":
		t.Kind = KindInt
	case "unsigned":
		t.Kind = KindUint
	case "bool":
		t.Kind = KindBool
	case "null", "void", "pad":
		t.Kind = KindSkip
	}
	if n > 64 || (n == 0 && m[1] != "pad") {
		return Type{}, fmt.Errorf("binary type %q: size out of range", bt)
	}
	return t, nil
}

// Dimension sources in asizeBT (4-41).
const (
	DimFixed = iota // a number in the descriptor
	DimHere         // "#": in the fixed area, after the pointer
	DimThere        // "##": in the variable area, where the pointer points
)

// Dim is one dimension of an array.
type Dim struct {
	N     int // for DimFixed
	Where int
}

// Array is a parsed asizeBT attribute. Ptr is 0 for an array in the fixed
// area, or the pointer width in bits. Only BPTR pointers are supported: they
// are the only kind the gateway sends (4-41).
type Array struct {
	Ptr  int
	Dims []Dim
}

// ParseArray reads an asizeBT attribute.
func ParseArray(s string) (*Array, error) {
	f := strings.Fields(s)
	if len(f) == 0 {
		return nil, fmt.Errorf("empty asizeBT")
	}
	a := &Array{}
	if kind, width, ok := strings.Cut(f[0], "-"); ok && strings.HasSuffix(kind, "PTR") {
		if kind != "BPTR" {
			return nil, fmt.Errorf("asizeBT %q: only BPTR pointers are supported", s)
		}
		w, err := strconv.Atoi(width)
		if err != nil || (w != 8 && w != 16 && w != 32 && w != 64) {
			return nil, fmt.Errorf("asizeBT %q: bad pointer width", s)
		}
		a.Ptr = w
		f = f[1:]
	}
	for _, d := range f {
		switch d {
		case "#":
			a.Dims = append(a.Dims, Dim{Where: DimHere})
		case "##":
			a.Dims = append(a.Dims, Dim{Where: DimThere})
		default:
			n, err := strconv.Atoi(d)
			if err != nil || n < 0 {
				return nil, fmt.Errorf("asizeBT %q: bad size %q", s, d)
			}
			a.Dims = append(a.Dims, Dim{N: n})
		}
	}
	for _, d := range a.Dims {
		if d.Where != DimFixed && a.Ptr == 0 {
			return nil, fmt.Errorf("asizeBT %q: # without a pointer", s)
		}
	}
	return a, nil
}

// fixedBits is the size the array takes in the area that holds it: the
// elements for an inline array, or the pointer and any "#" sizes.
func (a *Array) fixedBits(elemBits int) int {
	if a.Ptr == 0 {
		n := 1
		for _, d := range a.Dims {
			n *= d.N
		}
		return n * elemBits
	}
	n := 1
	for _, d := range a.Dims {
		if d.Where == DimHere {
			n++
		}
	}
	return n * a.Ptr
}

// Null-pointer flavours (4-45, 4-46): the 32 largest values of the pointer
// width. Index 0 is the largest.
var nullFlavors = []string{"NULL", "NI", "NA", "UNK", "NASK", "ASKU", "NAV", "OTH",
	"PINF", "NINF", "MSK", "INV", "MISS", "SKIP", "DELETED"}

// nullFlavor returns the flavour of pointer value p of the given width, or ""
// if p is a real offset.
func nullFlavor(p uint64, bits int) string {
	max := uint64(1)<<bits - 1
	if bits == 64 {
		max = ^uint64(0)
	}
	if p < max-31 {
		return ""
	}
	if i := int(max - p); i < len(nullFlavors) {
		return nullFlavors[i]
	}
	return "reserved"
}
