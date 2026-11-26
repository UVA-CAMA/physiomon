package mllp

import (
	"bytes"
	"strings"
	"testing"
)

func TestReadWriteRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	msgs := []string{"MSH|^~\\&|A|B\nPID|1", "MSH|^~\\&|C|D\rOBX|1"}
	for _, m := range msgs {
		if err := Write(&buf, []byte(m)); err != nil {
			t.Fatal(err)
		}
	}
	buf.WriteString("noise before start block")
	Write(&buf, []byte("third"))
	for _, want := range append(msgs, "third") {
		got, err := Read(&buf)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}
}

func TestReadRejectsMissingCR(t *testing.T) {
	_, err := Read(strings.NewReader("\x0bMSH|x\x1cX"))
	if err == nil || !strings.Contains(err.Error(), "expected CR") {
		t.Fatalf("got %v, want an expected-CR error", err)
	}
}

func TestBuildACKEchoesControlID(t *testing.T) {
	ack := string(BuildACK([]byte("MSH|^~\\&|S|F|R|F|20260101||ORU^R01|CTRL42|P|2.6\nPID|1")))
	if !strings.HasSuffix(ack, "\rMSA|AA|CTRL42\r") {
		t.Errorf("ack %q does not echo MSH-10", ack)
	}
}
