package mime

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"strconv"
)

// Content types the gateway uses (manual 3-3).
const (
	TypeXML    = "application/x-sapphire+xml"
	TypeBinary = "application/x-sapphire+binary"
)

// OutPart is a part to be written.
type OutPart struct {
	ContentType string
	Body        []byte
	// Extra header lines, written before Content-Length. Used by the fake
	// gateway to test long headers.
	Extra []string
}

// NewBoundary returns a random boundary, a version 4 UUID.
func NewBoundary() string {
	b := make([]byte, 16)
	rand.Read(b)
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	h := hex.EncodeToString(b)
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}

// Encode writes a multipart message in the layout the gateway accepts: top-level
// headers, a blank line, then for each part a boundary, its headers, a blank
// line, the body and a CRLF, and finally the closing boundary with its CRLF.
func Encode(boundary string, parts ...OutPart) []byte {
	var body bytes.Buffer
	for _, p := range parts {
		body.WriteString("--" + boundary + "\r\n")
		for _, x := range p.Extra {
			body.WriteString(x + "\r\n")
		}
		body.WriteString("Content-Length: " + strconv.Itoa(len(p.Body)) + "\r\n")
		body.WriteString("Content-Type: " + p.ContentType + "\r\n")
		body.WriteString("Content-Transfer-Encoding: binary\r\n\r\n")
		body.Write(p.Body)
		body.WriteString("\r\n")
	}
	body.WriteString("--" + boundary + "--\r\n")

	var out bytes.Buffer
	out.WriteString("Mime-Version: 1.0\r\n")
	out.WriteString("Content-Type: multipart/mixed;boundary=\"" + boundary + "\"\r\n")
	out.WriteString("Content-Length: " + strconv.Itoa(body.Len()) + "\r\n\r\n")
	out.Write(body.Bytes())
	return out.Bytes()
}
