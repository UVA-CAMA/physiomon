package xbin

import (
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
)

// be builds a big-endian frame from uint8, int8, uint16, int16, uint32,
// int32, uint64, int64, float32, float64 and []byte values.
func be(vals ...any) []byte {
	var b []byte
	for _, v := range vals {
		switch x := v.(type) {
		case []byte:
			b = append(b, x...)
		default:
			b, _ = binary.Append(b, binary.BigEndian, x)
		}
	}
	return b
}

func mustParse(t *testing.T, doc string) *Descriptor {
	t.Helper()
	d, err := Parse([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func mustDecode(t *testing.T, d *Descriptor, buf []byte) *Frame {
	t.Helper()
	f, err := d.Decode(buf)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// field returns the values of the field at path, failing if there is not
// exactly one such field.
func field(t *testing.T, d *Descriptor, f *Frame, path string) (*Field, Values) {
	t.Helper()
	var found []int
	for i, fl := range d.Fields {
		if fl.Path == path {
			found = append(found, i)
		}
	}
	if len(found) != 1 {
		t.Fatalf("%d fields at %s", len(found), path)
	}
	return d.Fields[found[0]], f.Values[found[0]]
}

func ints(t *testing.T, got Values, want ...int64) {
	t.Helper()
	if !slices.Equal(got.Ints, want) {
		t.Errorf("values %v, want %v", got.Ints, want)
	}
}

const header = `
<formatID BT="xs:unsignedInt" iBV="55"/>
<sessionID BT="xs:unsignedByte" asizeBT="12"/>
<blockSQN BT="xs:unsignedInt" iBV="1"/>
<blockLength BT="xs:unsignedShort"/>
<startDateTime BT="xb:NTP-32" INV="0"/>`

var session = []byte{0x7E, 0x14, 0x64, 0x55, 0, 0, 0x07, 0xD0, 0x4C, 0xAC, 0x9B, 0xCD}

// headerBytes is the 26-byte block header with the given length.
func headerBytes(length uint16) []byte {
	return be(uint32(0x40001234), session, uint32(0x123), length, uint32(0x01345498))
}

func desc(params string) string {
	return `<s:sapphire xmlns:s="urn:ge:sapphire:sapphire_1"><binDescriptor xmlns="urn:ge:sapphire:binDesc_1">
<header xmlns="urn:ge:sapphire:base_1"><sessionID V="7E146455000007D04CAC9BCD"/><msgCHN V="1"/></header>
<block U="Hz" rate="1" repeat="1">` + header + `<groupBT repeat="1"><params xmlns="urn:ge:sapphire:param_1">` +
		params + `</params></groupBT></block></binDescriptor></s:sapphire>`
}

// The episodic example on manual pages 4-30 and 4-31. Its frames begin
// 04 00 12 34, a format ID with the wrong top bits (SUMMARY section 14); the
// decoder reads what the descriptor says and leaves that check to its caller.
func TestManualEpisodic(t *testing.T) {
	d := mustParse(t, desc(`
<ecg index="1" source="ge:unityDevice"><num><heartRate BT="xs:short"/></num></ecg>
<nbp index="1" source="ge:unityDevice"><num>
  <nbpMeasurements asizeBT="BPTR-16 ##"><episodic>
    <dateTime BT="xb:NTP-32"/><systolic BT="xs:short"/><mean BT="xs:short"/><diastolic BT="xs:short"/><pulseRate BT="xs:short"/>
  </episodic></nbpMeasurements>
  <cuffPressure BT="xs:short"/>
</num></nbp>`))
	if d.FormatID != 55 || d.FixedBytes != 32 {
		t.Fatalf("format ID %d, fixed area %d bytes, want 55 and 32", d.FormatID, d.FixedBytes)
	}
	hdr := be(uint32(0x04001234), session, uint32(0x123), uint16(0x44), uint32(0x01345498))

	none := mustDecode(t, d, be(hdr, int16(0x50), uint16(0xFFFF), int16(0x42)))
	if none.FormatID != 0x04001234 || none.Session != [12]byte(session) || none.BlockSQN != 0x123 || none.BlockLength != 0x44 || none.Start != 0x01345498 {
		t.Errorf("header %+v", none)
	}
	_, hr := field(t, d, none, "groupBT/params/ecg/num/heartRate")
	ints(t, hr, 0x50)
	sys, v := field(t, d, none, "groupBT/params/nbp/num/nbpMeasurements/episodic/systolic")
	if v.Null != "NULL" || len(v.Ints) != 0 {
		t.Errorf("no episodic data: %+v", v)
	}
	if !sys.Repeated || sys.SignalPath() != "nbp/num/nbpMeasurements/episodic/systolic" || sys.Param != "nbp" || sys.Service != "num" {
		t.Errorf("field %+v", sys)
	}
	_, cuff := field(t, d, none, "groupBT/params/nbp/num/cuffPressure")
	ints(t, cuff, 0x42)

	one := mustDecode(t, d, be(hdr, int16(0x50), uint16(0x20), int16(0x42),
		uint16(1), uint32(0x01251E32), int16(120), int16(90), int16(70), int16(65)))
	_, dt := field(t, d, one, "groupBT/params/nbp/num/nbpMeasurements/episodic/dateTime")
	ints(t, dt, 0x01251E32)
	_, v = field(t, d, one, "groupBT/params/nbp/num/nbpMeasurements/episodic/diastolic")
	ints(t, v, 70)
	_, cuff = field(t, d, one, "groupBT/params/nbp/num/cuffPressure")
	ints(t, cuff, 0x42)

	two := mustDecode(t, d, be(hdr, int16(0x50), uint16(0x20), int16(0x42), uint16(2),
		uint32(1), int16(120), int16(90), int16(70), int16(65),
		uint32(2), int16(121), int16(91), int16(71), int16(66)))
	_, v = field(t, d, two, "groupBT/params/nbp/num/nbpMeasurements/episodic/pulseRate")
	ints(t, v, 65, 66)
	if none.End != 32 || one.End != 32+2+12 || two.End != 32+2+24 {
		t.Errorf("ends %d %d %d", none.End, one.End, two.End)
	}
}

// A waveform shape seen from a real gateway,
// whose frames are 941 bytes.
func TestRealWaveform(t *testing.T) {
	inv := `INV="-32768 -32753 -32761 -32762 -32763 -32767 -32752" mapINV="INV MISS MISS_HWMGR_DATA MISS_RACK_DATA MISS_RX_DATA MISS_TX_DATA MISS_NET_DATA" INV_LE="-32752"`
	var leads strings.Builder
	for _, l := range []string{"I", "II", "III", "V1", "AVR", "AVL", "AVF"} {
		fmt.Fprintf(&leads, `<ecgWaveform U="uV" S="2.44" BT="xs:short" %s asizeBT="60" lead="%s"/>`, inv, l)
	}
	marker := `U="{bool}" S="1.0" BT="xb:bool-8" INV="-128 -127" mapINV="INV MISS" INV_LE="-127" asizeBT="15"`
	d := mustParse(t, desc(`<ecg source="ge:unityDevice" index="1"><wav><ecgWaveformMXG>`+leads.String()+`</ecgWaveformMXG>
<defibSyncMarker `+marker+`/><qrsMarker `+marker+`/><paceMarker `+marker+`/></wav></ecg>
<spO2 site="unknown" source="ge:unityDevice" index="1"><wav><pleth U="{unitless}" S="1.0" BT="xs:short" `+inv+` asizeBT="15"/></wav></spO2>`))
	if d.FixedBytes != 941 {
		t.Fatalf("fixed area %d bytes, want 941", d.FixedBytes)
	}
	if d.SessionID != "7E146455000007D04CAC9BCD" {
		t.Errorf("session ID %q", d.SessionID)
	}
	buf := headerBytes(941)
	for lead := range 7 {
		for i := range 60 {
			buf = be(buf, int16(lead*100+i))
		}
	}
	buf = be(buf, make([]byte, 15), []byte{1}, make([]byte, 14), []byte{0x80}, make([]byte, 14), make([]byte, 30))
	f := mustDecode(t, d, buf)
	var leadII *Field
	var leadIIValues Values
	for i, fl := range d.Fields {
		if fl.Attrs["lead"] == "II" {
			leadII, leadIIValues = fl, f.Values[i]
		}
	}
	if len(leadIIValues.Ints) != 60 || leadIIValues.Ints[0] != 100 || leadIIValues.Ints[59] != 159 {
		t.Errorf("lead II %v", leadIIValues.Ints)
	}
	if leadII.Scale != 2.44 || leadII.Unit != "uV" || leadII.SignalPath() != "ecg/wav/ecgWaveformMXG/ecgWaveform" {
		t.Errorf("lead II field %+v", leadII)
	}
	for v, want := range map[int64]string{-32768: "INV", -32753: "MISS", -32762: "MISS_RACK_DATA", -32760: "INV", -32752: "MISS_NET_DATA", -32751: "", 0: ""} {
		if got, _ := leadII.Invalid(v); got != want {
			t.Errorf("Invalid(%d) = %q, want %q", v, got, want)
		}
	}
	qrs, v := field(t, d, f, "groupBT/params/ecg/wav/qrsMarker")
	if v.Ints[0] != 1 || len(v.Ints) != 15 {
		t.Errorf("qrs %v", v.Ints)
	}
	_, pace := field(t, d, f, "groupBT/params/ecg/wav/paceMarker")
	if pace.Ints[0] != -128 {
		t.Errorf("bool-8 0x80 read as %d, want -128", pace.Ints[0])
	}
	if r, bad := qrs.Invalid(-128); !bad || r != "INV" {
		t.Errorf("bool-8 -128 not invalid")
	}
	pleth, _ := field(t, d, f, "groupBT/params/spO2/wav/pleth")
	if pleth.ParamAttrs["site"] != "unknown" || pleth.ParamAttrs["index"] != "1" {
		t.Errorf("pleth parameter attributes %v", pleth.ParamAttrs)
	}
}

// Real numeric parameters: invasive pressure with
// episodic wedge pressure, cardiac output with an attribute the manual does
// not list, and an offset the decoder must report as unknown.
func TestRealNumericAttributes(t *testing.T) {
	inv := `INV="-32768 -32767" mapINV="INV MISS" INV_LE="-32752"`
	d := mustParse(t, desc(`
<ip source="ge:unityDevice" index="4" site="pulmonary_artery" label="PA"><num>
  <pawMeasurements asizeBT="BPTR-16 ##"><episodic><dateTime BT="xb:NTP-32" INV="0"/><paw U="mm[Hg]" BT="xs:short" `+inv+`/></episodic></pawMeasurements>
  <systolic U="mm[Hg]" BT="xs:short" `+inv+`/>
</num><wav><ipWaveform U="mm[Hg]" S="0.5" BT="xs:short" offset="UNK" asizeBT="2"/></wav></ip>
<co source="ge:unityDevice" index="1"><num>
  <coMeasurements asizeBT="BPTR-16 ##"><episodic><dateTime BT="xb:NTP-32" INV="0"/>
    <cardiacOutput U="L/min" S="0.1" BT="xs:short" `+inv+` averageValue="true"/></episodic></coMeasurements>
</num></co>
<ecgResp source="ge:unityDevice" index="1"><num><respRate U="{resp}/min" BT="xs:short" `+inv+` respMethod="impedance" respSite="II"/></num></ecgResp>`))
	buf := be(headerBytes(0), uint16(0xFFF3), int16(25), int16(10), int16(11), uint16(38), int16(16))
	buf = be(buf, uint16(1), uint32(99), int16(52))
	f := mustDecode(t, d, buf)
	_, paw := field(t, d, f, "groupBT/params/ip/num/pawMeasurements/episodic/paw")
	if paw.Null != "MISS" {
		t.Errorf("paw pointer 0xFFF3: null %q, want MISS", paw.Null)
	}
	sys, v := field(t, d, f, "groupBT/params/ip/num/systolic")
	ints(t, v, 25)
	if sys.ParamAttrs["label"] != "PA" || sys.ParamAttrs["index"] != "4" {
		t.Errorf("ip attributes %v", sys.ParamAttrs)
	}
	wav, v := field(t, d, f, "groupBT/params/ip/wav/ipWaveform")
	ints(t, v, 10, 11)
	if !wav.OffsetUnknown || wav.Scale != 0.5 {
		t.Errorf("ipWaveform scale %v offset unknown %v", wav.Scale, wav.OffsetUnknown)
	}
	co, v := field(t, d, f, "groupBT/params/co/num/coMeasurements/episodic/cardiacOutput")
	ints(t, v, 52)
	if co.Attrs["averageValue"] != "true" || co.Scale != 0.1 {
		t.Errorf("cardiac output %+v", co)
	}
	rr, v := field(t, d, f, "groupBT/params/ecgResp/num/respRate")
	ints(t, v, 16)
	if rr.Attrs["respMethod"] != "impedance" || rr.Attrs["respSite"] != "II" {
		t.Errorf("respRate attributes %v", rr.Attrs)
	}
}

// Manual 4-32.
func TestEnum(t *testing.T) {
	d := mustParse(t, desc(`<resp source="ge:unityDevice" index="1"><cfg>
<ventMode BT="xs:short" INV="-1" mapINV="UNK"><enumList>
  <enum token="CMV" eBV="0"/><enum token="IMV" eBV="1"/><enum token="CSV" eBV="2"/><enum token="manual" eBV="3"/>
</enumList></ventMode><tidalVolume BT="xs:short"/></cfg></resp>`))
	f := mustDecode(t, d, be(headerBytes(30), int16(2), int16(500)))
	vm, v := field(t, d, f, "groupBT/params/resp/cfg/ventMode")
	if vm.Enum[v.Ints[0]] != "CSV" || len(vm.Enum) != 4 {
		t.Errorf("ventMode %v -> %q", v.Ints, vm.Enum[v.Ints[0]])
	}
	if r, bad := vm.Invalid(-1); !bad || r != "UNK" {
		t.Errorf("ventMode -1: %q %v", r, bad)
	}
	_, v = field(t, d, f, "groupBT/params/resp/cfg/tidalVolume")
	ints(t, v, 500)
}

// Manual 4-38 and 4-39.
func TestBitFields(t *testing.T) {
	d := mustParse(t, desc(`<x source="s" index="1"><num>
<parStatusBITS bitFieldOrder="bigEndian">
  <lead_1_fail BT="xb:bool-1"/><lead_2_fail BT="xb:bool-1"/><lead_3_fail BT="xb:bool-1"/>
  <UNUSED BT="xb:void-12"/><lead_RL_fail BT="xb:bool-1"/>
</parStatusBITS>
<pacemaker><paceDetectCh1 BT="xb:unsigned-2" asizeBT="4"/></pacemaker>
<interleavedPacemaker asizeBT="2"><pacemaker>
  <paceDetectCh1 BT="xb:unsigned-2"/><paceDetectCh2 BT="xb:unsigned-2"/><paceDetectCh3 BT="xb:signed-2"/><void BT="xb:void-2"/>
</pacemaker></interleavedPacemaker>
<after BT="xs:short"/>
</num></x>`))
	if d.FixedBytes != 26+2+1+2+2 {
		t.Fatalf("fixed area %d", d.FixedBytes)
	}
	// 1010 0000 0000 0001 | 00 01 10 11 | 01 10 11 00, 11 00 01 00 | after
	f := mustDecode(t, d, be(headerBytes(0), []byte{0xA0, 0x01, 0x1B, 0x6C, 0xC4}, int16(-5)))
	for path, want := range map[string][]int64{
		"lead_1_fail": {1}, "lead_2_fail": {0}, "lead_3_fail": {1}, "lead_RL_fail": {1},
	} {
		_, v := field(t, d, f, "groupBT/params/x/num/parStatusBITS/"+path)
		ints(t, v, want...)
	}
	_, v := field(t, d, f, "groupBT/params/x/num/pacemaker/paceDetectCh1")
	ints(t, v, 0, 1, 2, 3)
	_, v = field(t, d, f, "groupBT/params/x/num/interleavedPacemaker/pacemaker/paceDetectCh2")
	ints(t, v, 2, 0)
	_, v = field(t, d, f, "groupBT/params/x/num/interleavedPacemaker/pacemaker/paceDetectCh3")
	ints(t, v, -1, 1)
	_, v = field(t, d, f, "groupBT/params/x/num/after")
	ints(t, v, -5)
}

// The array forms on manual pages 4-42 to 4-44.
func TestManualArrays(t *testing.T) {
	d := mustParse(t, desc(`<x source="s" index="1"><num>
<vector BT="xs:short" asizeBT="4"/>
<array2d BT="xs:short" asizeBT="2 3"/>
<array1 asizeBT="2"><array2 BT="xs:short" asizeBT="2"/></array1>
<strHere BT="xb:UTF-8" asizeBT="BPTR-16 #"/>
<strThere BT="xb:UTF-8" asizeBT="BPTR-16 ##"/>
<dyn2d BT="xs:short" asizeBT="BPTR-16 # #"/>
<mixed BT="xs:short" asizeBT="BPTR-16 3 #"/>
<strings asizeBT="2"><str BT="xb:UTF-8" asizeBT="BPTR-16 ##"/></strings>
<jagged asizeBT="BPTR-16 ##"><foo BT="xs:short" asizeBT="BPTR-16 #"/></jagged>
</num></x>`))
	// Fixed area after the header (26 bytes): vector 8, array2d 12, array1 8,
	// strHere 4, strThere 2, dyn2d 6, mixed 4, strings 4, jagged 2 = 50 bytes.
	if d.FixedBytes != 76 {
		t.Fatalf("fixed area %d, want 76", d.FixedBytes)
	}
	fixed := be(headerBytes(0),
		int16(1), int16(2), int16(3), int16(4),
		int16(11), int16(12), int16(13), int16(21), int16(22), int16(23),
		int16(5), int16(6), int16(7), int16(8),
		uint16(76), uint16(5), // strHere: "hello" at 76
		uint16(81),                       // strThere: count and "good-bye!" at 81
		uint16(92), uint16(2), uint16(2), // dyn2d: 2x2 at 92
		uint16(100), uint16(1), // mixed: 3x1 at 100
		uint16(106), uint16(0xFFFF), // strings: "Iam" at 106, then null
		uint16(111), // jagged at 111
	)
	vari := be([]byte("hello"), uint16(9), []byte("good-bye!"),
		int16(31), int16(32), int16(33), int16(34),
		int16(41), int16(42), int16(43),
		uint16(3), []byte("Iam"),
		uint16(2), uint16(121), uint16(2), uint16(125), uint16(1), // jagged: 2 rows, pointers with # counts
		int16(51), int16(52), int16(53))
	f := mustDecode(t, d, be(fixed, vari))
	str := func(v Values) string {
		b := make([]byte, len(v.Ints))
		for i, c := range v.Ints {
			b[i] = byte(c)
		}
		return string(b)
	}
	for path, want := range map[string][]int64{
		"vector": {1, 2, 3, 4}, "array2d": {11, 12, 13, 21, 22, 23}, "array1/array2": {5, 6, 7, 8},
		"dyn2d": {31, 32, 33, 34}, "mixed": {41, 42, 43}, "jagged/foo": {51, 52, 53},
	} {
		_, v := field(t, d, f, "groupBT/params/x/num/"+path)
		if !slices.Equal(v.Ints, want) {
			t.Errorf("%s: %v, want %v", path, v.Ints, want)
		}
	}
	_, v := field(t, d, f, "groupBT/params/x/num/strHere")
	if str(v) != "hello" {
		t.Errorf("strHere %q", str(v))
	}
	_, v = field(t, d, f, "groupBT/params/x/num/strThere")
	if str(v) != "good-bye!" {
		t.Errorf("strThere %q", str(v))
	}
	_, v = field(t, d, f, "groupBT/params/x/num/strings/str")
	if str(v) != "Iam" || v.Null != "NULL" {
		t.Errorf("strings %q, null %q", str(v), v.Null)
	}
}

func TestNullFlavors(t *testing.T) {
	for p, want := range map[uint64]string{0xFFFF: "NULL", 0xFFFE: "NI", 0xFFF4: "INV", 0xFFF3: "MISS", 0xFFF1: "DELETED", 0xFFE0: "reserved", 0xFFDF: "", 0: ""} {
		if got := nullFlavor(p, 16); got != want {
			t.Errorf("0x%X: %q, want %q", p, got, want)
		}
	}
	if nullFlavor(0xFF, 8) != "NULL" || nullFlavor(math.MaxUint64, 64) != "NULL" || nullFlavor(0xFFFFFFF3, 32) != "MISS" {
		t.Error("other widths")
	}
}

// Unknown elements are skipped by their size; a type whose size cannot be
// known rejects the descriptor (5-2, 4-35).
func TestMustIgnore(t *testing.T) {
	d := mustParse(t, desc(`<x source="s" index="1"><num>
<a BT="xs:short"/>
<future xmlns="urn:example:future_9"><widget BT="xb:widget-24"/><big BT="xs:unsigned-128"/></future>
<b BT="xs:short"/>
</num></x>`))
	f := mustDecode(t, d, be(headerBytes(0), int16(1), []byte{9, 9, 9}, make([]byte, 16), int16(2)))
	_, v := field(t, d, f, "groupBT/params/x/num/b")
	ints(t, v, 2)
	_, v = field(t, d, f, "groupBT/params/x/num/future/widget")
	if len(v.Ints) != 0 {
		t.Errorf("opaque type decoded: %v", v.Ints)
	}
	for _, bad := range []string{
		`<x source="s" index="1"><num><a BT="xs:decimal"/></num></x>`,
		`<x source="s" index="1"><num><a BT="xs:short" asizeBT="APTR-32 ##"/></num></x>`,
		`<x source="s" index="1"><num><a BT="xs:short" asizeBT="#"/></num></x>`,
		`<x source="s" index="1"><num><s bitFieldOrder="littleEndian"><a BT="xb:bool-1"/><p BT="xb:pad-7"/></s></num></x>`,
		`<x source="s" index="1"><num><a BT="xb:bool-1"/></num></x>`,
		`<x source="s" index="1"><num><a BT="xs:short" S="1/0"/></num></x>`,
	} {
		if _, err := Parse([]byte(desc(bad))); err == nil {
			t.Errorf("accepted %s", bad)
		}
	}
}

func TestDecodeErrors(t *testing.T) {
	d := mustParse(t, desc(`<x source="s" index="1"><num><m asizeBT="BPTR-16 ##"><e><v BT="xs:int"/></e></m></num></x>`))
	for name, buf := range map[string][]byte{
		"short":             headerBytes(0)[:20],
		"pointer outside":   be(headerBytes(0), uint16(500)),
		"count too large":   be(headerBytes(0), uint16(28), uint16(1000)),
		"count past end":    be(headerBytes(0), uint16(28), uint16(2), int32(1)),
		"size past the end": be(headerBytes(0), uint16(28), []byte{0}),
	} {
		if _, err := d.Decode(buf); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}

func TestTypes(t *testing.T) {
	d := mustParse(t, desc(`<x source="s" index="1"><num>
<b BT="xs:byte"/><ub BT="xs:unsignedByte"/><s BT="xs:short"/><us BT="xs:unsignedShort"/>
<i BT="xs:int"/><ui BT="xs:unsignedInt"/><l BT="xs:long"/><ul BT="xs:unsignedLong" INV="18446744073709551615"/>
<fl BT="xs:float" INV="-1.5"/><db BT="xs:double"/><bo BT="xs:boolean"/><t64 BT="xb:NTP-64"/>
<r BT="xs:short" S="10/1024" offset="-3.5"/>
</num></x>`))
	f := mustDecode(t, d, be(headerBytes(0), int8(-2), uint8(200), int16(-300), uint16(60000),
		int32(-70000), uint32(4000000000), int64(-5), uint64(math.MaxUint64), float32(-1.5), float64(2.25),
		uint8(1), uint64(0x0000000180000000), int16(1024)))
	for path, want := range map[string]int64{"b": -2, "ub": 200, "s": -300, "us": 60000, "i": -70000, "ui": 4000000000, "l": -5, "bo": 1, "t64": 0x180000000} {
		_, v := field(t, d, f, "groupBT/params/x/num/"+path)
		ints(t, v, want)
	}
	ul, v := field(t, d, f, "groupBT/params/x/num/ul")
	if uint64(v.Ints[0]) != math.MaxUint64 {
		t.Errorf("unsignedLong %d", uint64(v.Ints[0]))
	}
	if _, bad := ul.Invalid(v.Ints[0]); !bad {
		t.Error("unsignedLong INV not matched")
	}
	fl, v := field(t, d, f, "groupBT/params/x/num/fl")
	if v.Floats[0] != -1.5 {
		t.Errorf("float %v", v.Floats)
	}
	if _, bad := fl.InvalidFloat(-1.5); !bad {
		t.Error("float INV not matched")
	}
	_, v = field(t, d, f, "groupBT/params/x/num/db")
	if v.Floats[0] != 2.25 {
		t.Errorf("double %v", v.Floats)
	}
	r, _ := field(t, d, f, "groupBT/params/x/num/r")
	if r.Scale != 10.0/1024 || r.Offset != -3.5 {
		t.Errorf("S %v offset %v", r.Scale, r.Offset)
	}
}

// Real ventilator numerics have elements with BT
// inside elements with BT, such as respRate/spontaneous. Each element's
// value comes before those of the elements inside it, in document order.
func TestNestedSignals(t *testing.T) {
	d := mustParse(t, desc(`<resp source="ge:unityDevice" index="1"><num>
<expiredMinuteVolume U="mL/min" S="100" BT="xs:short"><spontaneous U="mL/min" S="100" BT="xs:short"/></expiredMinuteVolume>
<PEEP><dynamic U="cm[H2O]" BT="xs:short"><intrinsic U="cm[H2O]" BT="xs:short"/><extrinsic U="cm[H2O]" BT="xs:short"/></dynamic></PEEP>
<respRate U="{resp}/min" BT="xs:short"><spontaneous U="{resp}/min" BT="xs:short"/></respRate>
</num></resp>`))
	if d.FixedBytes != 26+2*7 {
		t.Fatalf("fixed area %d bytes, want %d", d.FixedBytes, 26+2*7)
	}
	f := mustDecode(t, d, be(headerBytes(26+2*7), []int16{1, 2, 3, 4, 5, 6, 7}))
	for path, want := range map[string]int64{
		"groupBT/params/resp/num/expiredMinuteVolume":             1,
		"groupBT/params/resp/num/expiredMinuteVolume/spontaneous": 2,
		"groupBT/params/resp/num/PEEP/dynamic":                    3,
		"groupBT/params/resp/num/PEEP/dynamic/intrinsic":          4,
		"groupBT/params/resp/num/PEEP/dynamic/extrinsic":          5,
		"groupBT/params/resp/num/respRate":                        6,
		"groupBT/params/resp/num/respRate/spontaneous":            7,
	} {
		_, v := field(t, d, f, path)
		ints(t, v, want)
	}
	if f.End != len(be(headerBytes(0), []int16{1, 2, 3, 4, 5, 6, 7})) {
		t.Errorf("end %d", f.End)
	}
}
