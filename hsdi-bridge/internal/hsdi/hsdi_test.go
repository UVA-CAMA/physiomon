package hsdi

import (
	"net"
	"os"

	"github.com/UVA-CAMA/physiomon/hsdi-bridge/internal/mime"
	"testing"
	"time"
)

// From manual page 3-26, with the typographic quotes corrected.
const binEnd = `<?xml version="1.0" encoding="UTF-8"?>
<s:sapphire xmlns:s="urn:ge:sapphire:sapphire_1" version="0.0">
   <binEnd xmlns="urn:ge:sapphire:binEnd_1" message="Client too slow.">
       <header xmlns="urn:ge:sapphire:base_1">
           <sessionID V="0800190000001234C7627B00"/>
           <msgCHN V="1"/>
           <msgSQN V="9"/>
           <responseID V="500" more="."/>
           <creationDateTime V="2002-10-10T12:00:00-05:00"/>
       </header>
       <block>
           <formatID V="7"/>
           <blockSQN V="2400"/>
       </block>
   </binEnd>
</s:sapphire>`

// From manual page 3-11.
const helloNak = `<sapphire>
  <helloResponse status="nak_unauthorized" xmlns="urn:ge:sapphire:helloMsg_1" errorString="No available connections." >
        <header xmlns="urn:ge:sapphire:base_1">
            <sessionID V="080019000000123456789ABC"/>
            <msgCHN V="0"/>
            <replytoMsgSQN V="467"/>
            <creationDateTime V="2001-12-17T09:30:47.0Z"/>
        </header>
        <device><globalID V="FFFFFFFFFFFFFFFF"/></device>
    </helloResponse>
</sapphire>`

func TestParseBinEnd(t *testing.T) {
	h, err := ParseHeader([]byte(binEnd))
	if err != nil {
		t.Fatal(err)
	}
	want := Header{Root: "binEnd", Message: "Client too slow.", SessionID: "0800190000001234C7627B00",
		Chn: 1, MsgSQN: 9, ReplyToMsgSQN: -1, RequestID: -1, ResponseID: 500, More: "."}
	if h != want {
		t.Errorf("got  %+v\nwant %+v", h, want)
	}
}

func TestParseHelloNak(t *testing.T) {
	h, err := ParseHeader([]byte(helloNak))
	if err != nil {
		t.Fatal(err)
	}
	if h.Root != "helloResponse" || !h.IsNak() || h.ErrorString != "No available connections." || h.ReplyToMsgSQN != 467 {
		t.Errorf("got %+v", h)
	}
}

func TestParseRejectsOtherRoot(t *testing.T) {
	if _, err := ParseHeader([]byte(`<other><x/></other>`)); err == nil {
		t.Error("want error")
	}
}

// The builders must write exactly the expected bytes in testdata/, the
// layout the gateways accept.
func TestWireFormat(t *testing.T) {
	now := time.Unix(1767323045, 0)
	sid := ClientSessionID(&net.TCPAddr{IP: net.IPv4(10, 1, 2, 3), Port: 2007}, now)
	if sid != "0A010203000007D7695735A5" {
		t.Fatalf("session ID = %s", sid)
	}
	rh := func(sqn, req int64) RequestHeader {
		return RequestHeader{SessionID: sid, SessionType: ClientSessionType, MsgSQN: sqn, RequestID: req, ReplyTo: -1, Time: now}
	}
	gw := Header{SessionID: "0800190000001234C7627B00", Chn: 1, MsgSQN: 9, ResponseID: 500}
	cases := map[string][]byte{
		"hello":                   Hello(rh(1, 0)),
		"getSessionUpdateRequest": GetSessionUpdateRequest(rh(2, 1)),
		"getStreamRequest_wav":    GetStreamRequest(rh(3, 2), []string{"*"}, SelectWaveform),
		"getStreamRequest_num":    GetStreamRequest(rh(4, 3), []string{"*"}, SelectNumeric),
		"endStreamRequest":        EndStreamRequest(rh(5, 4), []string{"2", "3"}),
		"endSessionUpdateRequest": EndSessionUpdateRequest(rh(6, 5), []string{"1"}),
		"keepAliveRequest":        KeepAliveRequest(rh(7, 0)),
		"genericResponse":         GenericResponse(gw, now),
	}
	for name, got := range cases {
		want, err := os.ReadFile("testdata/wire_" + name + ".xml")
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != string(want) {
			t.Errorf("%s differs from testdata.\ngot:\n%s\nwant:\n%s", name, got, want)
		}
		if _, err := ParseHeader(got); err != nil {
			t.Errorf("%s does not parse: %v", name, err)
		}
	}
}

func TestTimeFormat(t *testing.T) {
	if got := formatTime(time.Date(2009, 6, 18, 21, 7, 37, 119e6, time.UTC)); got != "2009-06-18T21:07:37.119Z" {
		t.Errorf("got %s", got)
	}
	if got := formatTime(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)); got != "2026-01-02T03:04:05Z" {
		t.Errorf("got %s", got)
	}
}

// The whole MIME message for hello, as sent on the socket, with a fixed
// boundary.
func TestHelloMIME(t *testing.T) {
	want, err := os.ReadFile("testdata/wire_hello.mime")
	if err != nil {
		t.Fatal(err)
	}
	const boundary = "2667d9f8-7719-470d-b036-9425defc99de"
	now := time.Unix(1767323045, 0)
	sid := ClientSessionID(&net.TCPAddr{IP: net.IPv4(10, 1, 2, 3), Port: 2007}, now)
	doc := Hello(RequestHeader{SessionID: sid, SessionType: ClientSessionType, MsgSQN: 1, ReplyTo: -1, Time: now})
	got := mime.Encode(boundary, mime.OutPart{ContentType: mime.TypeXML, Body: doc})
	if string(got) != string(want) {
		t.Errorf("got:\n%q\nwant:\n%q", got, want)
	}
}

// A real gateway's keep-alive request (session ID invented) and our answer.
func TestKeepAliveResponseToGateway(t *testing.T) {
	req := `<?xml version="1.0" encoding="UTF-8"?>
<s:sapphire xmlns:s="urn:ge:sapphire:sapphire_1" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" version="2.0.1">
 <keepAliveRequest xmlns="urn:ge:sapphire:keepAliveMsg_1">
  <header xmlns="urn:ge:sapphire:base_1">
   <sessionID V="0A0102030000CE5D6AF1E9B1" type="IP-32:PORT-32:NTPS-32"/>
   <msgCHN V="0"/>
   <msgSQN V="36"/>
   <requestID V="15619"/>
   <creationDateTime V="2026-01-02T03:04:05.093Z"/>
  </header>
 </keepAliveRequest>
</s:sapphire>
`
	h, err := ParseHeader([]byte(req))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := ParseHeader(KeepAliveResponse(h, time.Unix(1767323045, 0)))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Root != "keepAliveResponse" || resp.Status != "ack" || resp.SessionID != "0A0102030000CE5D6AF1E9B1" ||
		resp.Chn != 0 || resp.ReplyToMsgSQN != 36 || resp.ResponseID != 15619 || resp.More != "." {
		t.Errorf("response %+v", resp)
	}
}
