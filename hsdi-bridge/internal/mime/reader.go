// Package mime reads and writes the MIME framing the HSDI gateway uses
// on its TCP connection (HSDI manual 3-2 to 3-4).
//
// The reader trusts each part's Content-Length and the boundary lines, and
// does not depend on the overall Content-Length. It has no limit on header
// lines, and after a framing error it discards only up to the next line that
// starts a message.
package mime

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
)

// Limits that stop a corrupt stream from making the reader allocate without
// bound. None of them is set by the manual.
const (
	DefaultMaxHeaderLines = 200
	DefaultMaxLineBytes   = 64 << 10
	DefaultMaxPartBytes   = 64 << 20
	MaxDiscardKept        = 1 << 20
	maxBlankLines         = 16
)

// Header holds MIME header fields. Keys are lower case; values are trimmed.
type Header map[string]string

// Get returns the value of a field, matching the name without regard to case.
func (h Header) Get(name string) string { return h[strings.ToLower(name)] }

// Part is one MIME part: its header fields and its body.
type Part struct {
	Header      Header
	HeaderLines int
	Body        []byte
}

// ContentType returns the part's media type without parameters, in lower case.
func (p Part) ContentType() string {
	ct, _, _ := strings.Cut(p.Header.Get("Content-Type"), ";")
	return strings.ToLower(strings.TrimSpace(ct))
}

// Message is one top-level MIME message.
type Message struct {
	Header      Header
	HeaderLines int
	Parts       []Part
	// Discarded is the number of bytes skipped before this message while
	// looking for its first line. DiscardedData holds them, up to
	// MaxDiscardKept bytes.
	Discarded     int
	DiscardedData []byte
	// FirstLine is the first header line of the message, as read.
	FirstLine string
	// Bare is set for an XML document that arrived with no MIME framing. A
	// real gateway sends its <keepAliveRequest> this way. It is returned as
	// one part with Content-Type TypeXML and the bytes as read.
	Bare bool
}

// FramingError reports a message that could not be read. Parts holds any parts
// of that message that were read completely before the error.
type FramingError struct {
	Reason        string
	Parts         []Part
	Discarded     int
	DiscardedData []byte
}

func (e *FramingError) Error() string { return "mime framing: " + e.Reason }

// Reader reads messages from a byte stream.
type Reader struct {
	br             *bufio.Reader
	MaxHeaderLines int
	MaxLineBytes   int
	MaxPartBytes   int
	// pending is a line already read that starts the next message.
	pending []byte
}

// NewReader returns a Reader with the default limits.
func NewReader(r io.Reader) *Reader {
	return &Reader{
		br:             bufio.NewReaderSize(r, 256<<10),
		MaxHeaderLines: DefaultMaxHeaderLines,
		MaxLineBytes:   DefaultMaxLineBytes,
		MaxPartBytes:   DefaultMaxPartBytes,
	}
}

var errLineTooLong = errors.New("line too long")

// readLine returns the next line without its line ending, and the number of
// bytes consumed. A line longer than MaxLineBytes is consumed in full and
// reported with errLineTooLong.
func (r *Reader) readLine() ([]byte, int, error) {
	line, n, err := r.readRawLine()
	return bytes.TrimRight(line, "\r\n"), n, err
}

// readRawLine is readLine with the line ending kept.
func (r *Reader) readRawLine() ([]byte, int, error) {
	var line []byte
	n := 0
	tooLong := false
	for {
		chunk, err := r.br.ReadSlice('\n')
		n += len(chunk)
		if !tooLong {
			if len(line)+len(chunk) > r.MaxLineBytes {
				tooLong = true
				line = nil
			} else {
				line = append(line, chunk...)
			}
		}
		if err == bufio.ErrBufferFull {
			continue
		}
		if err != nil {
			if err == io.EOF && n > 0 {
				err = io.ErrUnexpectedEOF
			}
			return nil, n, err
		}
		break
	}
	if tooLong {
		return nil, n, errLineTooLong
	}
	return line, n, nil
}

func hasPrefixFold(line []byte, prefix string) bool {
	return len(line) >= len(prefix) && strings.EqualFold(string(line[:len(prefix)]), prefix)
}

// isMessageStart reports a line that can only start a new message.
func isMessageStart(line []byte) bool { return hasPrefixFold(line, "mime-version:") }

// isHeaderStart reports a line that may start a message when one is
// expected. Top-level header lines may come in any order, so Mime-Version
// need not come first.
func isHeaderStart(line []byte) bool {
	return isMessageStart(line) || hasPrefixFold(line, "content-type:") || hasPrefixFold(line, "content-length:")
}

// Next reads the next message. It returns io.EOF when the stream ends cleanly
// between messages, a *FramingError for a message that could not be read, and
// any other error from the underlying reader. After a *FramingError, Next may
// be called again; it skips to the next line that starts a message.
func (r *Reader) Next() (*Message, error) {
	discarded := 0
	var kept []byte
	keep := func(line []byte, n int) {
		discarded += n
		if len(kept) < MaxDiscardKept {
			if line == nil {
				kept = append(kept, fmt.Sprintf("[%d bytes in an over-long line]\n", n)...)
			} else {
				kept = append(kept, line...)
				kept = append(kept, '\n')
			}
		}
	}
	var first []byte
	if r.pending != nil {
		first, r.pending = r.pending, nil
	} else {
		for {
			raw, n, err := r.readRawLine()
			line := bytes.TrimRight(raw, "\r\n")
			if err == errLineTooLong {
				keep(nil, n)
				continue
			}
			if err != nil {
				if err == io.ErrUnexpectedEOF {
					keep(nil, n)
					return nil, &FramingError{Reason: "stream ended inside garbage", Discarded: discarded, DiscardedData: kept}
				}
				return nil, err
			}
			if isHeaderStart(line) {
				first = line
				break
			}
			if bytes.HasPrefix(line, []byte("<?xml")) {
				body, err := r.readBareXML(raw)
				if err != nil {
					return nil, &FramingError{Reason: "XML without MIME framing: " + err.Error(), Discarded: discarded, DiscardedData: kept}
				}
				return &Message{Bare: true, Parts: []Part{{Header: Header{"content-type": TypeXML}, Body: body}},
					Discarded: discarded, DiscardedData: kept, FirstLine: string(line)}, nil
			}
			keep(line, n)
		}
	}

	msg := &Message{Discarded: discarded, DiscardedData: kept, FirstLine: string(first)}
	fail := func(format string, args ...any) (*Message, error) {
		return nil, &FramingError{Reason: fmt.Sprintf(format, args...), Parts: msg.Parts, Discarded: discarded, DiscardedData: kept}
	}

	hdr, lines, err := r.readHeader(first)
	if err != nil {
		return fail("message header: %v", err)
	}
	msg.Header, msg.HeaderLines = hdr, lines

	ctype := hdr.Get("Content-Type")
	boundary := boundaryOf(ctype)
	if boundary == "" {
		// Single part: the message header is also the part header.
		body, err := r.readBody(hdr)
		if err != nil {
			return fail("single-part body: %v", err)
		}
		msg.Parts = append(msg.Parts, Part{Header: hdr, HeaderLines: lines, Body: body})
		return msg, nil
	}

	delim := "--" + boundary
	// Find the first boundary, allowing blank lines before it.
	if kind, err := r.readBoundary(delim); err != nil {
		return fail("first boundary: %v", err)
	} else if kind != boundaryNext {
		return fail("message has no parts")
	}
	for {
		ph, plines, err := r.readHeader(nil)
		if err != nil {
			return fail("part %d header: %v", len(msg.Parts)+1, err)
		}
		body, err := r.readBody(ph)
		if err != nil {
			return fail("part %d body: %v", len(msg.Parts)+1, err)
		}
		msg.Parts = append(msg.Parts, Part{Header: ph, HeaderLines: plines, Body: body})
		kind, err := r.readBoundary(delim)
		if err != nil {
			return fail("after part %d: %v", len(msg.Parts), err)
		}
		if kind == boundaryEnd {
			return msg, nil
		}
	}
}

var rootTag = regexp.MustCompile(`<([A-Za-z_][\w.:-]*)`)

// readBareXML reads an XML document that arrived without MIME framing, up to
// the line that closes its root element. first is its first line, already
// read, with its line ending. The result keeps every byte read.
func (r *Reader) readBareXML(first []byte) ([]byte, error) {
	doc := append([]byte(nil), first...)
	end := ""
	for {
		if end == "" {
			if m := rootTag.FindSubmatch(doc); m != nil {
				end = "</" + string(m[1]) + ">"
			}
		}
		if end != "" && bytes.Contains(doc, []byte(end)) {
			return doc, nil
		}
		if len(doc) > MaxDiscardKept {
			return nil, fmt.Errorf("no %s within %d bytes", end, MaxDiscardKept)
		}
		raw, _, err := r.readRawLine()
		if err != nil {
			return nil, err
		}
		if isMessageStart(raw) {
			r.pending = bytes.TrimRight(raw, "\r\n")
			return nil, errors.New("a MIME message started inside it")
		}
		doc = append(doc, raw...)
	}
}

// readHeader reads header lines up to and including the blank line that ends
// them. first, if not nil, is a line already read.
func (r *Reader) readHeader(first []byte) (Header, int, error) {
	h := Header{}
	lines := 0
	blanks := 0
	lastKey := ""
	add := func(line []byte) error {
		lines++
		if lines > r.MaxHeaderLines {
			return fmt.Errorf("more than %d header lines", r.MaxHeaderLines)
		}
		if (line[0] == ' ' || line[0] == '\t') && lastKey != "" {
			h[lastKey] += " " + strings.TrimSpace(string(line))
			return nil
		}
		k, v, ok := strings.Cut(string(line), ":")
		if !ok {
			return fmt.Errorf("header line without a colon")
		}
		lastKey = strings.ToLower(strings.TrimSpace(k))
		h[lastKey] = strings.TrimSpace(v)
		return nil
	}
	if first != nil {
		if err := add(first); err != nil {
			return nil, lines, err
		}
	}
	for {
		line, _, err := r.readLine()
		if err != nil {
			return nil, lines, err
		}
		if len(line) == 0 {
			if lines == 0 {
				// Blank lines before a part header; tolerated.
				if blanks++; blanks > maxBlankLines {
					return nil, lines, errors.New("too many blank lines before header")
				}
				continue
			}
			return h, lines, nil
		}
		if isMessageStart(line) && first == nil {
			r.pending = line
			return nil, lines, errors.New("new message started inside a part header")
		}
		if err := add(line); err != nil {
			return nil, lines, err
		}
	}
}

func (r *Reader) readBody(h Header) ([]byte, error) {
	cl := h.Get("Content-Length")
	if cl == "" {
		return nil, errors.New("no Content-Length")
	}
	n, err := strconv.Atoi(cl)
	if err != nil || n < 0 {
		return nil, fmt.Errorf("bad Content-Length %q", cl)
	}
	if n > r.MaxPartBytes {
		return nil, fmt.Errorf("Content-Length %d over limit %d", n, r.MaxPartBytes)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r.br, body); err != nil {
		return nil, err
	}
	return body, nil
}

type boundaryKind int

const (
	boundaryNext boundaryKind = iota
	boundaryEnd
)

// readBoundary skips blank lines and reads a boundary line. Boundaries are
// compared without regard to case, because the manual's own example on 3-3
// ends with an upper-case boundary.
func (r *Reader) readBoundary(delim string) (boundaryKind, error) {
	for blanks := 0; ; blanks++ {
		line, _, err := r.readLine()
		if err != nil {
			return 0, err
		}
		s := strings.TrimSpace(string(line))
		if s == "" {
			if blanks >= maxBlankLines {
				return 0, errors.New("too many blank lines before boundary")
			}
			continue
		}
		switch {
		case strings.EqualFold(s, delim+"--"):
			return boundaryEnd, nil
		case strings.EqualFold(s, delim):
			return boundaryNext, nil
		case isHeaderStart(line):
			r.pending = line
			return 0, errors.New("new message started where a boundary was expected")
		default:
			return 0, errors.New("expected boundary")
		}
	}
}

// boundaryOf returns the boundary parameter of a multipart Content-Type, or ""
// if the type is not multipart. Straight and typographic quotes are removed.
func boundaryOf(ctype string) string {
	mt, params, _ := strings.Cut(ctype, ";")
	if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(mt)), "multipart/") {
		return ""
	}
	for _, p := range strings.Split(params, ";") {
		k, v, ok := strings.Cut(p, "=")
		if ok && strings.EqualFold(strings.TrimSpace(k), "boundary") {
			return strings.Trim(strings.TrimSpace(v), "\"“”")
		}
	}
	return ""
}
