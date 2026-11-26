package report

import (
	"bytes"
	"strings"
	"testing"
)

const respDescriptor = `<s:sapphire xmlns:s="urn:ge:sapphire:sapphire_1"><binDescriptor>
  <header><sessionID V="0800190000001234C7627B00"/></header>
  <block rate="1" U="Hz" repeat="1">
    <formatID BT="xs:unsignedInt" iBV="55"/>
    <startDateTime BT="xb:NTP-32" INV="0"/>
    <groupBT repeat="1"><params>
      <ecgResp source="ge:unityDevice" index="1"><num>
        <respRate U="{resp}/min" BT="xs:short" respMethod="impedance" respSite="chest" newThing="x"/>
      </num></ecgResp>
      <ip source="ge:unityDevice" index="1" label="ICP"><num>
        <cerebralPerfusionPressure U="mm[Hg]" S="0.1" BT="xs:short"/>
      </num></ip>
    </params></groupBT>
  </block>
</binDescriptor></s:sapphire>`

func TestManualPaths(t *testing.T) {
	var num, cfg, wav int
	for _, p := range ManualPaths() {
		switch strings.Split(p, "/")[1] {
		case "num":
			num++
		case "cfg":
			cfg++
		case "wav":
			wav++
		default:
			t.Errorf("bad path %q", p)
		}
	}
	if num != 138 || cfg != 20 || wav != 6 {
		t.Errorf("num %d cfg %d wav %d, want 138 20 6", num, cfg, wav)
	}
}

func TestCoverage(t *testing.T) {
	c := NewCoverage(DescriptorAllow(true))
	for _, d := range []string{descriptor, respDescriptor, respDescriptor} {
		if err := c.Add("0800190000001234C7627B00", []byte(d)); err != nil {
			t.Fatal(err)
		}
	}
	var b bytes.Buffer
	c.Write(&b)
	out := b.String()
	checkNoPHI(t, out)
	seen, notSeen, _ := strings.Cut(out, "In the manual, not seen")
	notSeen, other, _ := strings.Cut(notSeen, "Seen, not in the manual")
	for _, want := range []string{
		`ip/num/cerebralPerfusionPressure`,
		`ecgResp/num/respRate`,
		`@respMethod (attribute not in the manual): "impedance"×2`,
		`@newThing (attribute not in the manual): "…"×2`,
		`ip/wav/ipWaveform`,
		`"xs:short ×30"×2`,
	} {
		if !strings.Contains(seen, want) {
			t.Errorf("seen section missing %q:\n%s", want, seen)
		}
	}
	if !strings.Contains(seen, "manual paths seen: 3 of 164; other paths seen: 0") {
		t.Errorf("wrong totals:\n%s", seen)
	}
	if !strings.Contains(notSeen, "nbp/num/cuffPressure") || strings.Contains(notSeen, "ip/wav/ipWaveform") {
		t.Errorf("not-seen section wrong:\n%s", notSeen)
	}
	if strings.TrimSpace(other) != "" {
		t.Errorf("unexpected other paths:\n%s", other)
	}
	// One descriptor per path, not one per instance: two ip channels in one
	// descriptor count once.
	if !strings.Contains(seen, "ip/wav/ipWaveform") || !strings.Contains(out, "ip/wav/ipWaveform                                                 1     1") {
		t.Errorf("descriptor count wrong:\n%s", seen)
	}
}
