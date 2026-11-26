package session

import (
	"fmt"
	"testing"
)

// Invented values, in the shape of real session updates.
func update(status, id, patient string) []byte {
	return fmt.Appendf(nil, `<?xml version="1.0" encoding="UTF-8"?>
<s:sapphire xmlns:s="urn:ge:sapphire:sapphire_1" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" version="2.0.1">
<sessionUpdate xmlns="urn:ge:sapphire:sessionUpdateMsg_1">
<header xmlns="urn:ge:sapphire:base_1"><sessionID V="7EC46464000007D74A3AAC99"/></header>
<sessionList xmlns="urn:ge:sapphire:base_1"><session status="%s">
  <sessionID V="%s" type="IP-32:PORT-32:NTPS-32"/>
  <deviceStatus>
    <assignedLocation xsi:type="ge.locType.Unity"><unitName V="DEVUNIT"/><bedName V="DEVBED"/></assignedLocation>
    <state><network V="ON_LINE"/><patientStatus V="ADMIT"/></state>
  </deviceStatus>
  <patientInfo>%s</patientInfo>
</session></sessionList></sessionUpdate></s:sapphire>`, status, id, patient)
}

func TestApply(t *testing.T) {
	tb := NewTable()
	const id = "7E146455000007D04CAC9BCD"
	identified := `<identifier type="ge.idType.UnityPID"><authority V="A"/><id V="OTHER"/></identifier>
<identifier type="ge.idType.UnityPID"><authority V="A"/><id V="MRN1"/><primary V="true"/></identifier>
<visit><assignedLocation xsi:type="ge.locType.Unity"><unitName V="UNIT1"/><bedName V="BED1"/></assignedLocation><patientData/></visit>`
	if _, err := tb.Apply(update("added", "7e146455000007d04cac9bcd", identified)); err != nil {
		t.Fatal(err)
	}
	s, ok := tb.Get(id)
	want := Session{ID: id, PatientID: "MRN1", Unit: "UNIT1", Bed: "BED1", PatientStatus: "ADMIT", Network: "ON_LINE"}
	if !ok || s != want {
		t.Errorf("got %+v, want %+v", s, want)
	}

	// An update replaces the session: here an unidentified patient, with an
	// identifier not marked primary, which is not used, and no visit
	// location, so the device's location is used.
	if _, err := tb.Apply(update("updated", id, `<unknownID V="true"/><identifier><id V="NOTPRIMARY"/></identifier>`)); err != nil {
		t.Fatal(err)
	}
	s, _ = tb.Get(id)
	want = Session{ID: id, UnknownID: true, Unit: "DEVUNIT", Bed: "DEVBED", PatientStatus: "ADMIT", Network: "ON_LINE"}
	if s != want {
		t.Errorf("got %+v, want %+v", s, want)
	}

	removed, err := tb.Apply(update("removed", id, ""))
	if err != nil || len(removed) != 1 || removed[0] != id {
		t.Fatal(removed, err)
	}
	if _, ok := tb.Get(id); ok || tb.Len() != 0 {
		t.Error("removed session kept")
	}
	if _, err := tb.Apply([]byte("<not xml")); err == nil {
		t.Error("no error for bad XML")
	}
}
