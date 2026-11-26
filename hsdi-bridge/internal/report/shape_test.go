package report

import (
	"bytes"
	"strings"
	"testing"
)

// Adapted from manual page 4-23: two invasive pressure parameters.
const descriptor = `<?xml version="1.0" encoding="UTF-8"?>
<s:sapphire xmlns:s="urn:ge:sapphire:sapphire_1" version="0.0">
<binDescriptor xmlns="urn:ge:sapphire:binDesc_1">
  <header xmlns="urn:ge:sapphire:base_1">
    <sessionID V="0800190000001234C7627B00"/><msgCHN V="2"/><msgSQN V="2"/>
    <creationDateTime V="2002-10-10T12:00:00-05:00"/>
  </header>
  <block rate="4" U="Hz" repeat="1">
    <formatID BT="xs:unsignedInt" iBV="539"/>
    <sessionID BT="xs:unsignedByte" asizeBT="12"/>
    <groupBT repeat="1"><params xmlns="urn:ge:sapphire:param_1">
      <ip source="ge:unityDevice" index="1" label="ART" site="artery"><wav>
        <ipWaveform BT="xs:short" asizeBT="30" S="0.2" U="mm[Hg]" INV="-32768 -32767" mapINV="INV MISS"/>
      </wav></ip>
      <ip source="ge:unityDevice" index="2" label="PA" site="pulmonary_artery"><wav>
        <ipWaveform BT="xs:short" asizeBT="30" S="0.5" U="mm[Hg]" offset="UNK"/>
      </wav></ip>
    </params></groupBT>
  </block>
</binDescriptor></s:sapphire>`

// Adapted from manual page 3-35, with invented values.
const sessionUpdate = `<s:sapphire xmlns:s="urn:ge:sapphire:sapphire_1" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
<sessionUpdate>
  <header xmlns="urn:ge:sapphire:base_1"><sessionID V="7EC46464000007D74A3AAC99"/><creationDateTime V="2009-06-18T21:07:37.824Z"/></header>
  <sessionList><session>
    <sessionID V="7E01E17C000007D04A3AAC85" type="IP-32:PORT-32:NTPS-32"/>
    <startTime V="2009-06-18T21:07:17.792Z"/>
    <deviceStatus>
      <assignedLocation xsi:type="ge.locType.Unity"><unitName V="NOWEST"/><bedName V="RM1"/></assignedLocation>
      <services><service V="wav"/><service V="num"/></services>
      <state><network V="ON_LINE"/><patientStatus V="ADMIT"/></state>
    </deviceStatus>
    <patientInfo>
      <name use="ABC"><given V="IMA"/><family V="PATIENT"/></name>
      <identifier><id V="DCBA123"/></identifier>
      <birthDateTime V="1950-01-01"/>
      <note>free text SMITH</note>
    </patientInfo>
  </session></sessionList>
</sessionUpdate></s:sapphire>`

var phi = []string{"0800190000001234C7627B00", "2002-10-10", "7E01E17C000007D04A3AAC85", "2009-06-18", "NOWEST", "RM1", "IMA", "PATIENT", "DCBA123", "1950", "SMITH", "ABC"}

func checkNoPHI(t *testing.T, out string) {
	t.Helper()
	for _, s := range phi {
		if strings.Contains(out, s) {
			t.Errorf("output contains %q:\n%s", s, out)
		}
	}
}

func TestDescriptorTree(t *testing.T) {
	out, err := Tree([]byte(descriptor), DescriptorAllow(true))
	if err != nil {
		t.Fatal(err)
	}
	checkNoPHI(t, out)
	for _, want := range []string{
		`binDescriptor`,
		`  block rate="4" U="Hz" repeat="1"`,
		`    formatID BT="xs:unsignedInt" iBV="…"`,
		`ip source="ge:unityDevice" index="2" label="PA" site="pulmonary_artery"`,
		`ipWaveform BT="xs:short" asizeBT="30" S="0.5" U="mm[Hg]" offset="UNK"`,
		`mapINV="INV MISS"`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in\n%s", want, out)
		}
	}
	if strings.Contains(out, "header") || strings.Contains(out, "msgCHN") {
		t.Errorf("header not skipped:\n%s", out)
	}
	noLabels, _ := Tree([]byte(descriptor), DescriptorAllow(false))
	if strings.Contains(noLabels, `"ART"`) {
		t.Error("label shown when not allowed")
	}
}

func TestSessionUpdateSummary(t *testing.T) {
	s := NewPathSummary()
	if err := s.Add([]byte(sessionUpdate), SessionUpdateAllow); err != nil {
		t.Fatal(err)
	}
	var b bytes.Buffer
	s.Write(&b, 10)
	out := b.String()
	checkNoPHI(t, out)
	for _, want := range []string{
		`sessionUpdate/sessionList/session/deviceStatus/state/patientStatus`,
		`@V: "ADMIT"×1`,
		`@type: "IP-32:PORT-32:NTPS-32"×1`,
		`@type: "ge.locType.Unity"×1`,
		`sessionUpdate/sessionList/session/patientInfo/identifier/id`,
		`text 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in\n%s", want, out)
		}
	}
}

// From a real gateway's <binEnd>, with invented IDs.
func TestBinEndMessageRedacted(t *testing.T) {
	doc := `<s:sapphire xmlns:s="urn:ge:sapphire:sapphire_1"><binEnd message="BinEnd sent for sessionID 0800190000001234C7627B00 to client 7EC46464000007D74A3AAC99 as the session was removed from data acquisition. 2009-06-18T21:07:37.824Z"><header><sessionID V="0800190000001234C7627B00"/></header></binEnd></s:sapphire>`
	s := NewPathSummary()
	if err := s.Add([]byte(doc), OtherAllow); err != nil {
		t.Fatal(err)
	}
	var b bytes.Buffer
	s.Write(&b, 10)
	out := b.String()
	checkNoPHI(t, out)
	for _, bad := range []string{"7EC46464000007D74A3AAC99", "21:07"} {
		if strings.Contains(out, bad) {
			t.Errorf("output contains %q", bad)
		}
	}
	if !strings.Contains(out, "BinEnd sent for sessionID <session ID> to client <session ID> as the session was removed") {
		t.Errorf("message not shown redacted:\n%s", out)
	}
}

// Frames are counted by the channel of their descriptor; values are never
// printed.
func TestFramesByChannel(t *testing.T) {
	c := NewCollector(true)
	c.AddState([]byte(descriptor))
	frame := func(n int, first int16) []byte {
		b := []byte{0x40, 0, 0x02, 0x1B} // format ID 539
		b = append(b, 0x08, 0x00, 0x19, 0, 0, 0, 0x12, 0x34, 0xC7, 0x62, 0x7B, 0)
		for i := range n {
			v := int16(i)
			if i == 0 {
				v = first
			}
			b = append(b, byte(uint16(v)>>8), byte(v))
		}
		return b
	}
	c.Add("hsdi.raw.T.in.binary", nil, frame(60, -32768))
	c.Add("hsdi.raw.T.in.binary", nil, frame(60, 12345))
	c.Add("hsdi.raw.T.in.binary", nil, frame(10, 0))
	var b bytes.Buffer
	c.Write(&b)
	out := b.String()
	checkNoPHI(t, out)
	for _, want := range []string{
		"channel 2: 3 frames",
		"lengths: 136 B ×2, 36 B ×1",
		"frames with no matching descriptor: 0",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in\n%s", want, out)
		}
	}
	if strings.Contains(out, "12345") {
		t.Error("a value was printed")
	}
}
