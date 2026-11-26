package decode

import (
	"encoding/binary"
	"math"
	"sort"
)

// Encode returns f in protobuf wire format, as proto/decoded.proto defines
// it. Field numbers must match that file.
func Encode(f *Frame) []byte {
	var b []byte
	b = str(b, 1, f.Gateway)
	b = str(b, 2, f.SessionID)
	if p := f.Patient; p != nil {
		var m []byte
		m = str(m, 1, p.PatientID)
		m = boolean(m, 2, p.UnknownID)
		m = str(m, 3, p.Unit)
		m = str(m, 4, p.Bed)
		m = str(m, 5, p.PatientStatus)
		m = str(m, 6, p.Network)
		b = msg(b, 3, m)
	}
	b = uvarint(b, 4, uint64(f.FormatID))
	b = uvarint(b, 5, uint64(f.BlockSQN))
	b = int64Field(b, 6, f.StartTime)
	b = int64Field(b, 7, f.ReceivedMs)
	b = double(b, 8, f.RateHz)
	b = uvarint(b, 9, f.RawSeq)
	for i := range f.Signals {
		b = msg(b, 10, encodeSignal(&f.Signals[i]))
	}
	return b
}

func encodeSignal(s *Signal) []byte {
	var b []byte
	b = str(b, 1, s.Name)
	keys := make([]string, 0, len(s.Attrs))
	for k := range s.Attrs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		var e []byte
		e = str(e, 1, k)
		e = str(e, 2, s.Attrs[k])
		b = msg(b, 2, e)
	}
	b = str(b, 3, s.Unit)
	b = double(b, 4, s.Scale)
	b = double(b, 5, s.Offset)
	b = boolean(b, 6, s.OffsetUnknown)
	if len(s.Values) > 0 {
		var p []byte
		for _, v := range s.Values {
			p = binary.AppendUvarint(p, zigzag(v))
		}
		b = msg(b, 7, p)
	}
	if len(s.FloatValues) > 0 {
		var p []byte
		for _, v := range s.FloatValues {
			p = binary.LittleEndian.AppendUint64(p, math.Float64bits(v))
		}
		b = msg(b, 8, p)
	}
	if len(s.SamplesLE16) > 0 {
		b = msg(b, 9, s.SamplesLE16)
	}
	b = str(b, 10, s.Null)
	if len(s.Invalid) > 0 {
		var p []byte
		for _, v := range s.Invalid {
			p = binary.AppendUvarint(p, zigzag(v))
		}
		b = msg(b, 11, p)
	}
	for _, r := range s.InvalidReason {
		b = msg(b, 12, []byte(r)) // repeated string: one field each, even if empty
	}
	if s.InvalidLE != nil {
		b = optional(b, 13, *s.InvalidLE)
	}
	if s.InvalidGE != nil {
		b = optional(b, 14, *s.InvalidGE)
	}
	ekeys := make([]int64, 0, len(s.Enum))
	for k := range s.Enum {
		ekeys = append(ekeys, k)
	}
	sort.Slice(ekeys, func(i, j int) bool { return ekeys[i] < ekeys[j] })
	for _, k := range ekeys {
		var e []byte
		e = append(e, tag(1, 0)...)
		e = binary.AppendUvarint(e, zigzag(k))
		e = str(e, 2, s.Enum[k])
		b = msg(b, 15, e)
	}
	return b
}

// The helpers below omit fields with the default value, as proto3 does.

func tag(field int, wire byte) []byte {
	return binary.AppendUvarint(nil, uint64(field)<<3|uint64(wire))
}

func uvarint(b []byte, field int, v uint64) []byte {
	if v == 0 {
		return b
	}
	b = append(b, tag(field, 0)...)
	return binary.AppendUvarint(b, v)
}

// int64Field writes an int64 field, a plain varint.
func int64Field(b []byte, field int, v int64) []byte {
	return uvarint(b, field, uint64(v))
}

// optional writes an optional sint64 field, which is present even when 0.
func optional(b []byte, field int, v int64) []byte {
	b = append(b, tag(field, 0)...)
	return binary.AppendUvarint(b, zigzag(v))
}

func zigzag(v int64) uint64 { return uint64(v<<1) ^ uint64(v>>63) }

func boolean(b []byte, field int, v bool) []byte {
	if !v {
		return b
	}
	return append(append(b, tag(field, 0)...), 1)
}

func double(b []byte, field int, v float64) []byte {
	if v == 0 {
		return b
	}
	b = append(b, tag(field, 1)...)
	return binary.LittleEndian.AppendUint64(b, math.Float64bits(v))
}

func str(b []byte, field int, s string) []byte {
	if s == "" {
		return b
	}
	return msg(b, field, []byte(s))
}

func msg(b []byte, field int, m []byte) []byte {
	b = append(b, tag(field, 2)...)
	b = binary.AppendUvarint(b, uint64(len(m)))
	return append(b, m...)
}
