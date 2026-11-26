package mime

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

const helloXML = `<?xml version="1.0" encoding="UTF-8"?>
<s:sapphire xmlns:s="urn:ge:sapphire:sapphire_1" xmlns="urn:ge:sapphire:helloMsg_1" version="0.0"><hello/></s:sapphire>`

// The binary frame from manual page 3-3.
var frame33 = []byte{
	0x40, 0x00, 0x00, 0x01, 0x7E, 0x03, 0x6F, 0x63, 0x00, 0x00, 0x07, 0xD0, 0x49, 0xCC, 0xF8, 0x63,
	0x00, 0x00, 0x00, 0xA9, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0xFF, 0xFF, 0x00, 0x02, 0x00, 0x00,
	0x00, 0x02, 0xFF, 0xFF, 0x00, 0x01, 0xFF, 0xFF, 0x00, 0x02, 0x00, 0x01, 0x00, 0x03, 0x00, 0x01,
}

func TestRoundTrip(t *testing.T) {
	raw := Encode("b1", OutPart{ContentType: TypeXML, Body: []byte(helloXML)},
		OutPart{ContentType: TypeBinary, Body: frame33})
	r := NewReader(bytes.NewReader(raw))
	m, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Parts) != 2 {
		t.Fatalf("parts = %d", len(m.Parts))
	}
	if string(m.Parts[0].Body) != helloXML || m.Parts[0].ContentType() != TypeXML {
		t.Errorf("part 1 wrong: %q", m.Parts[0].ContentType())
	}
	if !bytes.Equal(m.Parts[1].Body, frame33) {
		t.Errorf("part 2 body wrong")
	}
	if _, err := r.Next(); err != io.EOF {
		t.Errorf("want EOF, got %v", err)
	}
}

// The manual's binary example (3-3): no blank line after the top-level
// headers, and a closing boundary in a different case.
func TestManualBinaryExample(t *testing.T) {
	var b bytes.Buffer
	b.WriteString("MIME-Version: 1.0\r\nContent-Type: multipart/mixed; boundary=\"gc0y0pkb9ex\"\r\nContent-Length: 175\r\n")
	b.WriteString("--gc0y0pkb9ex\r\nContent-Type: application/x-sapphire+binary\r\nContent-Transfer-Encoding: binary\r\nContent-Length: 48\r\n\r\n")
	b.Write(frame33)
	b.WriteString("\r\n--GC0Y0PKB9EX--\r\n")
	// Without the blank line the boundary is read as a header line, so this
	// is a framing error; the manual's own text says the blank line is
	// required. Check that the error is reported and the reader recovers.
	b.Write(Encode("x", OutPart{ContentType: TypeBinary, Body: frame33}))
	r := NewReader(&b)
	_, err := r.Next()
	var fe *FramingError
	if !errors.As(err, &fe) {
		t.Fatalf("want framing error, got %v", err)
	}
	m, err := r.Next()
	if err != nil {
		t.Fatalf("after recovery: %v", err)
	}
	if !bytes.Equal(m.Parts[0].Body, frame33) {
		t.Errorf("recovered body wrong")
	}
}

func TestTypographicQuoteAndUpperCaseEnd(t *testing.T) {
	var b bytes.Buffer
	b.WriteString("MIME-Version: 1.0\r\nContent-Type: multipart/mixed;  boundary=“gc0y0pkb9ex\"\r\nContent-Length: 1\r\n\r\n")
	b.WriteString("--gc0y0pkb9ex\r\nContent-Type: application/x-sapphire+binary\r\nContent-Length: 48\r\n\r\n")
	b.Write(frame33)
	b.WriteString("\r\n--GC0Y0PKB9EX--\r\n")
	m, err := NewReader(&b).Next()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(m.Parts[0].Body, frame33) {
		t.Errorf("body wrong")
	}
}

// More than ten header lines must be read.
func TestManyHeaderLines(t *testing.T) {
	var extra []string
	for i := 0; i < 25; i++ {
		extra = append(extra, fmt.Sprintf("X-Extra-%d: %d", i, i))
	}
	raw := Encode("b", OutPart{ContentType: TypeXML, Body: []byte(helloXML), Extra: extra})
	m, err := NewReader(bytes.NewReader(raw)).Next()
	if err != nil {
		t.Fatal(err)
	}
	if m.Parts[0].HeaderLines != 28 {
		t.Errorf("header lines = %d, want 28", m.Parts[0].HeaderLines)
	}
	if m.Parts[0].Header.Get("x-extra-24") != "24" {
		t.Errorf("last extra header missing")
	}
}

func TestGarbageBetweenMessages(t *testing.T) {
	one := Encode("b", OutPart{ContentType: TypeXML, Body: []byte(helloXML)})
	var b bytes.Buffer
	b.Write(one)
	b.WriteString("garbage line\r\n\x00\x01\x02 binary junk\r\n")
	b.Write(one)
	r := NewReader(&b)
	for i := 0; i < 2; i++ {
		m, err := r.Next()
		if err != nil {
			t.Fatalf("message %d: %v", i, err)
		}
		if i == 1 && m.Discarded == 0 {
			t.Errorf("discarded bytes not counted")
		}
	}
}

func TestWrongContentLengthRecovers(t *testing.T) {
	bad := Encode("b", OutPart{ContentType: TypeXML, Body: []byte(helloXML)})
	bad = bytes.Replace(bad, []byte(fmt.Sprintf("Content-Length: %d\r\nContent-Type", len(helloXML))),
		[]byte(fmt.Sprintf("Content-Length: %d\r\nContent-Type", len(helloXML)-10)), 1)
	good := Encode("c", OutPart{ContentType: TypeBinary, Body: frame33})
	r := NewReader(io.MultiReader(bytes.NewReader(bad), bytes.NewReader(good)))
	_, err := r.Next()
	var fe *FramingError
	if !errors.As(err, &fe) {
		t.Fatalf("want framing error, got %v", err)
	}
	m, err := r.Next()
	if err != nil {
		t.Fatalf("after recovery: %v", err)
	}
	if !bytes.Equal(m.Parts[0].Body, frame33) {
		t.Errorf("recovered body wrong")
	}
}

func TestPartsBeforeErrorAreKept(t *testing.T) {
	raw := Encode("b", OutPart{ContentType: TypeXML, Body: []byte(helloXML)},
		OutPart{ContentType: TypeBinary, Body: frame33})
	// Break the second part's Content-Length.
	raw = bytes.Replace(raw, []byte("Content-Length: 48"), []byte("Content-Length: x"), 1)
	_, err := NewReader(bytes.NewReader(raw)).Next()
	var fe *FramingError
	if !errors.As(err, &fe) {
		t.Fatalf("want framing error, got %v", err)
	}
	if len(fe.Parts) != 1 || string(fe.Parts[0].Body) != helloXML {
		t.Errorf("complete first part not kept: %d parts", len(fe.Parts))
	}
}

func TestSinglePart(t *testing.T) {
	raw := "Mime-Version: 1.0\r\nContent-Type: application/x-sapphire+xml\r\nContent-Length: " +
		fmt.Sprint(len(helloXML)) + "\r\n\r\n" + helloXML
	m, err := NewReader(strings.NewReader(raw)).Next()
	if err != nil {
		t.Fatal(err)
	}
	if string(m.Parts[0].Body) != helloXML {
		t.Errorf("body wrong")
	}
}

func TestTruncated(t *testing.T) {
	raw := Encode("b", OutPart{ContentType: TypeBinary, Body: frame33})
	_, err := NewReader(bytes.NewReader(raw[:len(raw)-30])).Next()
	if err == nil || err == io.EOF {
		t.Fatalf("want an error for a truncated message, got %v", err)
	}
}

// Top-level headers may come in any order. A message that starts with
// Content-Type must be read, not skipped.
func TestHeadersInAnyOrder(t *testing.T) {
	one := Encode("b", OutPart{ContentType: TypeBinary, Body: frame33})
	reordered := bytes.Replace(one, []byte("Mime-Version: 1.0\r\n"), nil, 1)
	reordered = bytes.Replace(reordered, []byte("Content-Length: "), []byte("Mime-Version: 1.0\r\nContent-Length: "), 1)
	if !bytes.HasPrefix(reordered, []byte("Content-Type:")) {
		t.Fatalf("test message not reordered: %q", reordered[:40])
	}
	r := NewReader(io.MultiReader(bytes.NewReader(one), bytes.NewReader(reordered), bytes.NewReader(one)))
	for i := 0; i < 3; i++ {
		m, err := r.Next()
		if err != nil {
			t.Fatalf("message %d: %v", i, err)
		}
		if m.Discarded != 0 || !bytes.Equal(m.Parts[0].Body, frame33) {
			t.Errorf("message %d: discarded %d", i, m.Discarded)
		}
	}
}

func TestDiscardedDataKept(t *testing.T) {
	var b bytes.Buffer
	b.WriteString("junk one\r\njunk two\r\n")
	b.Write(Encode("b", OutPart{ContentType: TypeBinary, Body: frame33}))
	m, err := NewReader(&b).Next()
	if err != nil {
		t.Fatal(err)
	}
	if string(m.DiscardedData) != "junk one\njunk two\n" || m.Discarded != 20 {
		t.Errorf("kept %q, discarded %d", m.DiscardedData, m.Discarded)
	}
}

// A real gateway sends <keepAliveRequest> as bare XML between MIME messages,
// as below (session ID invented).
const bareKeepAlive = `<?xml version="1.0" encoding="UTF-8"?>
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

func TestBareXML(t *testing.T) {
	one := Encode("b", OutPart{ContentType: TypeBinary, Body: frame33})
	for _, eol := range []string{"\n", "\r\n"} {
		bare := strings.ReplaceAll(bareKeepAlive, "\n", eol)
		r := NewReader(io.MultiReader(bytes.NewReader(one), strings.NewReader(bare), bytes.NewReader(one)))
		for i := 0; i < 3; i++ {
			m, err := r.Next()
			if err != nil {
				t.Fatalf("eol %q, message %d: %v", eol, i, err)
			}
			if m.Discarded != 0 {
				t.Errorf("message %d: %d bytes discarded", i, m.Discarded)
			}
			if i == 1 {
				if !m.Bare || len(m.Parts) != 1 || string(m.Parts[0].Body) != bare || m.Parts[0].ContentType() != TypeXML {
					t.Errorf("bare message %+v", m)
				}
			} else if m.Bare || !bytes.Equal(m.Parts[0].Body, frame33) {
				t.Errorf("message %d wrong", i)
			}
		}
	}
}

func TestBareXMLCutShort(t *testing.T) {
	one := Encode("b", OutPart{ContentType: TypeBinary, Body: frame33})
	cut := bareKeepAlive[:strings.Index(bareKeepAlive, "  </header>")]
	r := NewReader(io.MultiReader(strings.NewReader(cut), bytes.NewReader(one)))
	_, err := r.Next()
	var fe *FramingError
	if !errors.As(err, &fe) {
		t.Fatalf("want framing error, got %v", err)
	}
	m, err := r.Next()
	if err != nil || !bytes.Equal(m.Parts[0].Body, frame33) {
		t.Fatalf("after recovery: %v", err)
	}
}
