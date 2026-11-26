// Package hsdi reads the common header of HSDI XML messages and builds the
// messages a client sends (HSDI manual chapter 3).
//
// It reads only what the bridge needs: the message type, its status, and the
// common header fields. It does not interpret session updates or descriptors.
package hsdi

import (
	"bytes"
	"encoding/xml"
	"errors"
	"io"
	"strconv"
)

// Header is what the bridge reads from an XML message.
type Header struct {
	// Root is the local name of the message element, such as "binEnd".
	Root        string
	Status      string // root attribute "status", on responses
	ErrorString string // root attribute "errorString"
	Message     string // root attribute "message", on binEnd

	SessionID     string
	Chn           int   // msgCHN; -1 if absent
	MsgSQN        int64 // -1 if absent
	ReplyToMsgSQN int64 // -1 if absent
	RequestID     int64 // -1 if absent
	ResponseID    int64 // -1 if absent
	More          string
}

// IsNak reports whether Status is a nak, with or without a suffix (3-29).
func (h Header) IsNak() bool { return len(h.Status) >= 3 && h.Status[:3] == "nak" }

// ParseHeader reads the message type and common header of an XML message. It
// stops reading at the end of the header, so its cost does not grow with the
// size of the message body.
func ParseHeader(doc []byte) (Header, error) {
	h := Header{Chn: -1, MsgSQN: -1, ReplyToMsgSQN: -1, RequestID: -1, ResponseID: -1}
	d := xml.NewDecoder(bytes.NewReader(doc))
	depth := 0
	inHeader := false
	for {
		tok, err := d.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return h, err
		}
		switch t := tok.(type) {
		case xml.StartElement:
			depth++
			switch {
			case depth == 1:
				if t.Name.Local != "sapphire" {
					return h, errors.New("root element is " + t.Name.Local + ", not sapphire")
				}
			case depth == 2:
				h.Root = t.Name.Local
				h.Status = attr(t, "status")
				h.ErrorString = attr(t, "errorString")
				h.Message = attr(t, "message")
			case depth == 3 && t.Name.Local == "header":
				inHeader = true
			case depth == 4 && inHeader:
				v := attr(t, "V")
				switch t.Name.Local {
				case "sessionID":
					h.SessionID = v
				case "msgCHN":
					h.Chn = int(num(v))
				case "msgSQN":
					h.MsgSQN = num(v)
				case "replytoMsgSQN":
					h.ReplyToMsgSQN = num(v)
				case "requestID":
					h.RequestID = num(v)
				case "responseID":
					h.ResponseID = num(v)
					h.More = attr(t, "more")
				}
			}
		case xml.EndElement:
			if inHeader && depth == 3 {
				return h, nil
			}
			depth--
		}
	}
	if h.Root == "" {
		return h, errors.New("no message element")
	}
	return h, nil
}

func attr(e xml.StartElement, name string) string {
	for _, a := range e.Attr {
		if a.Name.Local == name {
			return a.Value
		}
	}
	return ""
}

func num(s string) int64 {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return -1
	}
	return n
}
