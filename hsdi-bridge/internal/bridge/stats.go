package bridge

import (
	"sync"
	"sync/atomic"
)

// Stats are counters for one gateway, since the process started.
type Stats struct {
	Connects       atomic.Int64
	Disconnects    atomic.Int64
	BytesIn        atomic.Int64
	MIMEMessages   atomic.Int64
	Parts          atomic.Int64
	XMLParts       atomic.Int64
	BinaryParts    atomic.Int64
	FramingErrors  atomic.Int64
	DiscardedBytes atomic.Int64
	XMLParseErrors atomic.Int64
	Sent           atomic.Int64
	Naks           atomic.Int64
	LateResponses  atomic.Int64
	BinEnds        atomic.Int64
	ReRequests     atomic.Int64
	// binEnds whose reason makes a re-request pointless
	BinEndsNotReRequested atomic.Int64
	PublishErrors         atomic.Int64
	StateMessages         atomic.Int64 // copies published to the state stream
	// XML documents that arrived without MIME framing, as the gateway's
	// keep-alive requests do
	BareMessages atomic.Int64
	// MaxHeaderLines is the most header lines seen in one message or part.
	MaxHeaderLines atomic.Int64
	// OverTenHeaderLines counts messages and parts with more than ten, a
	// limit some HSDI clients have.
	OverTenHeaderLines atomic.Int64

	roots sync.Map // message type -> *atomic.Int64
}

func (s *Stats) noteHeaderLines(n int) {
	if n > 10 {
		s.OverTenHeaderLines.Add(1)
	}
	for {
		cur := s.MaxHeaderLines.Load()
		if int64(n) <= cur || s.MaxHeaderLines.CompareAndSwap(cur, int64(n)) {
			return
		}
	}
}

func (s *Stats) noteRoot(root string) {
	v, ok := s.roots.Load(root)
	if !ok {
		v, _ = s.roots.LoadOrStore(root, new(atomic.Int64))
	}
	v.(*atomic.Int64).Add(1)
}

// Snapshot returns the counters as a map, for the status page and logs.
func (s *Stats) Snapshot() map[string]any {
	m := map[string]any{
		"connects":              s.Connects.Load(),
		"disconnects":           s.Disconnects.Load(),
		"bytesIn":               s.BytesIn.Load(),
		"mimeMessages":          s.MIMEMessages.Load(),
		"parts":                 s.Parts.Load(),
		"xmlParts":              s.XMLParts.Load(),
		"binaryParts":           s.BinaryParts.Load(),
		"framingErrors":         s.FramingErrors.Load(),
		"discardedBytes":        s.DiscardedBytes.Load(),
		"xmlParseErrors":        s.XMLParseErrors.Load(),
		"sent":                  s.Sent.Load(),
		"naks":                  s.Naks.Load(),
		"lateResponses":         s.LateResponses.Load(),
		"binEnds":               s.BinEnds.Load(),
		"reRequests":            s.ReRequests.Load(),
		"binEndsNotReRequested": s.BinEndsNotReRequested.Load(),
		"publishErrors":         s.PublishErrors.Load(),
		"stateMessages":         s.StateMessages.Load(),
		"bareMessages":          s.BareMessages.Load(),
		"maxHeaderLines":        s.MaxHeaderLines.Load(),
		"overTenHeaderLines":    s.OverTenHeaderLines.Load(),
	}
	roots := map[string]int64{}
	s.roots.Range(func(k, v any) bool {
		roots[k.(string)] = v.(*atomic.Int64).Load()
		return true
	})
	m["xmlByType"] = roots
	return m
}
