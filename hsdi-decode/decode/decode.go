// Package decode turns the raw stream's frames into decoded frames: values
// named by signal, with the patient and location from the session updates.
// It knows no signals; names, units, scales and invalid values all come from
// the descriptors, read by internal/xbin.
package decode

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/UVA-CAMA/physiomon/hsdi-decode/session"
	"github.com/UVA-CAMA/physiomon/hsdi-decode/xbin"
)

// Frame is one decoded binary frame. proto/decoded.proto documents the
// fields.
type Frame struct {
	Gateway    string
	SessionID  string
	Patient    *session.Session
	Service    string // wav if the frame has waveforms, otherwise num
	FormatID   uint32
	BlockSQN   uint32
	StartTime  int64 // Unix seconds
	ReceivedMs int64
	RateHz     float64
	RawSeq     uint64
	Signals    []Signal
}

// Signal is one field's values in one frame.
type Signal struct {
	Path          string // below params, such as ecg/num/heartRate
	Name          string // Path and qualifiers
	ScaleText     string // S as the descriptor writes it, "" if absent
	Attrs         map[string]string
	Unit          string
	Scale         float64
	Offset        float64
	OffsetUnknown bool
	Values        []int64
	FloatValues   []float64
	SamplesLE16   []byte
	Null          string
	Invalid       []int64
	InvalidReason []string
	InvalidLE     *int64
	InvalidGE     *int64
	Enum          map[int64]string
}

// qualifiers are the attributes that tell signals with the same path apart,
// on the element and on its parameter. In real descriptors, gases share a
// path and differ by respGas or aaGas, ECG leads by lead, and invasive
// pressure channels by their parameter's index.
var (
	fieldQualifiers = []string{"lead", "respGas", "aaGas"}
	paramQualifiers = []string{"index"}
	paramAttrs      = []string{"index", "label", "site", "source"}
)

// ntpToUnix is the number of seconds from 1900 to 1970.
const ntpToUnix = 2208988800

// Decoder holds the descriptors and sessions of every gateway.
type Decoder struct {
	gateways map[string]*gateway
	Stats    Stats
}

// Stats counts what the decoder did.
type Stats struct {
	Frames         int64 // decoded
	NoDescriptor   int64 // frames with no descriptor for their session and format ID
	DecodeErrors   int64 // frames that do not fit their descriptor
	NoSession      int64 // decoded frames whose session no session update named
	BadDescriptors int64 // descriptors xbin could not read
	BadUpdates     int64 // session updates that could not be read
}

type gateway struct {
	descs    map[string]*descriptor // session/formatID -> descriptor
	sessions *session.Table
}

type descriptor struct {
	d       *xbin.Descriptor
	service string
	rate    float64
	signals []Signal // metadata, without values, one per field with a SignalPath
	fields  []int    // index in d.Fields of each signal
}

// New returns an empty Decoder.
func New() *Decoder { return &Decoder{gateways: map[string]*gateway{}} }

func (d *Decoder) gateway(name string) *gateway {
	g := d.gateways[name]
	if g == nil {
		g = &gateway{descs: map[string]*descriptor{}, sessions: session.NewTable()}
		d.gateways[name] = g
	}
	return g
}

func key(session string, formatID uint32) string {
	return session + "/" + strconv.FormatUint(uint64(formatID&0x3FFFFFFF), 10)
}

// SessionUpdate applies a <sessionUpdate> from gateway gw. A removed
// session's descriptors are dropped with it.
func (d *Decoder) SessionUpdate(gw string, doc []byte) error {
	g := d.gateway(gw)
	removed, err := g.sessions.Apply(doc)
	if err != nil {
		d.Stats.BadUpdates++
		return err
	}
	for _, id := range removed {
		for k := range g.descs {
			if strings.HasPrefix(k, id+"/") {
				delete(g.descs, k)
			}
		}
	}
	return nil
}

// Descriptor applies a <binDescriptor> from gateway gw. It replaces any
// earlier descriptor for the same session and format ID.
func (d *Decoder) Descriptor(gw string, doc []byte) error {
	x, err := xbin.Parse(doc)
	if err != nil {
		d.Stats.BadDescriptors++
		return err
	}
	desc := &descriptor{d: x, service: "num"}
	if x.BlockAttrs["U"] == "Hz" {
		desc.rate, _ = strconv.ParseFloat(x.BlockAttrs["rate"], 64)
	}
	for i, f := range x.Fields {
		if f.SignalPath() == "" {
			continue
		}
		if f.Service == "wav" {
			desc.service = "wav"
		}
		desc.signals = append(desc.signals, signalMeta(f))
		desc.fields = append(desc.fields, i)
	}
	d.gateway(gw).descs[key(x.SessionID, x.FormatID)] = desc
	return nil
}

func signalMeta(f *xbin.Field) Signal {
	s := Signal{Path: f.SignalPath(), Name: f.SignalPath(), ScaleText: f.Attrs["S"], Attrs: map[string]string{}, Unit: f.Unit, Scale: f.Scale,
		Offset: f.Offset, OffsetUnknown: f.OffsetUnknown, Enum: f.Enum}
	for _, a := range fieldQualifiers {
		if v, ok := f.Attrs[a]; ok {
			s.Name += " " + a + "=" + v
			s.Attrs[a] = v
		}
	}
	for _, a := range paramQualifiers {
		if v, ok := f.ParamAttrs[a]; ok {
			s.Name += " " + a + "=" + v
		}
	}
	for _, a := range paramAttrs {
		if v, ok := f.ParamAttrs[a]; ok {
			s.Attrs[a] = v
		}
	}
	s.Invalid, s.InvalidReason, s.InvalidLE, s.InvalidGE = f.InvalidList()
	return s
}

// Frame decodes a binary frame from gateway gw. It returns nil, nil for a
// frame with no descriptor.
func (d *Decoder) Frame(gw string, data []byte, received time.Time, rawSeq uint64) (*Frame, error) {
	if len(data) < 16 {
		d.Stats.DecodeErrors++
		return nil, fmt.Errorf("frame is %d bytes", len(data))
	}
	g := d.gateway(gw)
	sess := strings.ToUpper(hex.EncodeToString(data[4:16]))
	desc := g.descs[key(sess, binary.BigEndian.Uint32(data))]
	if desc == nil {
		d.Stats.NoDescriptor++
		return nil, nil
	}
	x, err := desc.d.Decode(data)
	if err != nil {
		d.Stats.DecodeErrors++
		return nil, err
	}
	f := &Frame{Gateway: gw, SessionID: sess, Service: desc.service, FormatID: x.FormatID & 0x3FFFFFFF,
		BlockSQN: x.BlockSQN, ReceivedMs: received.UnixMilli(), RateHz: desc.rate, RawSeq: rawSeq,
		Signals: make([]Signal, len(desc.signals))}
	if x.Start != 0 {
		f.StartTime = int64(x.Start) - ntpToUnix
	}
	if s, ok := g.sessions.Get(sess); ok {
		f.Patient = &s
	} else {
		d.Stats.NoSession++
	}
	for i, fi := range desc.fields {
		s := desc.signals[i]
		v := x.Values[fi]
		s.Null = v.Null
		fl := desc.d.Fields[fi]
		switch {
		case fl.Type.Name == "xs:short" && fl.Array != nil:
			s.SamplesLE16 = make([]byte, 2*len(v.Ints))
			for j, n := range v.Ints {
				binary.LittleEndian.PutUint16(s.SamplesLE16[2*j:], uint16(int16(n)))
			}
		case len(v.Floats) > 0:
			s.FloatValues = v.Floats
		default:
			s.Values = v.Ints
		}
		f.Signals[i] = s
	}
	d.Stats.Frames++
	return f, nil
}
