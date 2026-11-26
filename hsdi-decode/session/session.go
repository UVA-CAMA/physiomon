// Package session keeps the gateway's sessions, from <sessionUpdate>
// messages (manual 3-33 to 3-38), so decoded frames can carry their patient
// and location.
//
// A session update lists sessions as added, updated or removed. An added or
// updated session replaces what was known about it; a removed session is
// dropped. The fields kept are the ones a reader needs to file a frame. They
// are PHI.
package session

import (
	"bytes"
	"encoding/xml"
	"strings"
)

// Session is what the latest session update said about one session.
type Session struct {
	ID string // session ID, 24 hex digits in upper case
	// PatientID is the identifier marked primary, such as the MRN. Empty if
	// none is marked primary, as for an unidentified patient.
	PatientID string
	UnknownID bool // the gateway marked the patient as unidentified
	// Unit and Bed are the patient's visit location, or the device's
	// location if the update has no visit location.
	Unit, Bed     string
	PatientStatus string // ADMIT or DISCHARGE
	Network       string // ON_LINE or OFF_LINE
}

// Table holds the sessions of one gateway.
type Table struct {
	m map[string]Session
}

// NewTable returns an empty Table.
func NewTable() *Table { return &Table{m: map[string]Session{}} }

// Get returns the session with ID id, upper case hex.
func (t *Table) Get(id string) (Session, bool) {
	s, ok := t.m[id]
	return s, ok
}

// Len returns the number of sessions.
func (t *Table) Len() int { return len(t.m) }

type location struct {
	Unit struct {
		V string `xml:"V,attr"`
	} `xml:"unitName"`
	Bed struct {
		V string `xml:"V,attr"`
	} `xml:"bedName"`
}

type value struct {
	V string `xml:"V,attr"`
}

type xmlSession struct {
	Status string `xml:"status,attr"`
	ID     value  `xml:"sessionID"`
	Device struct {
		Location location `xml:"assignedLocation"`
		State    struct {
			Network       value `xml:"network"`
			PatientStatus value `xml:"patientStatus"`
		} `xml:"state"`
	} `xml:"deviceStatus"`
	Patient struct {
		Identifiers []struct {
			ID      value `xml:"id"`
			Primary value `xml:"primary"`
		} `xml:"identifier"`
		UnknownID value `xml:"unknownID"`
		Visit     struct {
			Location location `xml:"assignedLocation"`
		} `xml:"visit"`
	} `xml:"patientInfo"`
}

// Apply reads a <sessionUpdate> message and updates the table. It returns
// the IDs of the sessions it removed.
func (t *Table) Apply(doc []byte) (removed []string, err error) {
	var msg struct {
		Sessions []xmlSession `xml:"sessionUpdate>sessionList>session"`
	}
	if err := xml.NewDecoder(bytes.NewReader(doc)).Decode(&msg); err != nil {
		return nil, err
	}
	for _, x := range msg.Sessions {
		id := strings.ToUpper(x.ID.V)
		if x.Status == "removed" {
			delete(t.m, id)
			removed = append(removed, id)
			continue
		}
		s := Session{
			ID:            id,
			UnknownID:     x.Patient.UnknownID.V == "true",
			Unit:          x.Patient.Visit.Location.Unit.V,
			Bed:           x.Patient.Visit.Location.Bed.V,
			PatientStatus: x.Device.State.PatientStatus.V,
			Network:       x.Device.State.Network.V,
		}
		if s.Unit == "" && s.Bed == "" {
			s.Unit, s.Bed = x.Device.Location.Unit.V, x.Device.Location.Bed.V
		}
		for _, p := range x.Patient.Identifiers {
			if p.Primary.V == "true" {
				s.PatientID = p.ID.V
				break
			}
		}
		t.m[id] = s
	}
	return removed, nil
}
