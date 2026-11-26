// Command fakegw runs the fake gateway from internal/fakegw, so the bridge can
// be run end to end on a workstation with no real gateway and no patient data.
//
//	fakegw -listen 127.0.0.1:2007 -sessions 4 -extra-headers 12
package main

import (
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/UVA-CAMA/physiomon/hsdi-bridge/internal/fakegw"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:2007", "address to listen on")
	var o fakegw.Options
	flag.IntVar(&o.Sessions, "sessions", 2, "number of fake beds")
	flag.DurationVar(&o.WaveInterval, "wave-interval", 250*time.Millisecond, "waveform frame interval per bed")
	flag.DurationVar(&o.NumericInterval, "numeric-interval", 2*time.Second, "numeric frame interval per bed")
	flag.IntVar(&o.ExtraHeaderLines, "extra-headers", 0, "extra MIME header lines on every part")
	flag.IntVar(&o.GarbageAfter, "garbage-after", 0, "write a line of garbage after this many waveform frames")
	flag.IntVar(&o.BinEndAfter, "binend-after", 0, "end the first bed's waveform stream after this many frames")
	flag.StringVar(&o.BinEndMessage, "binend-message", "", `reason in that binEnd (default "Client too slow.")`)
	flag.DurationVar(&o.BareKeepAliveEvery, "bare-keepalive-every", 0, "send a keep-alive request this often as XML without MIME framing, as a real gateway does")
	flag.BoolVar(&o.NakSessionRequests, "nak-session-requests", false, "refuse stream requests for named sessions, as a real gateway did")
	flag.IntVar(&o.DropAfter, "drop-after", 0, "close the first connection after this many frames")
	flag.BoolVar(&o.IgnoreKeepAlive, "ignore-keepalive", false, "do not answer keep-alive requests")
	flag.BoolVar(&o.IgnoreHello, "ignore-hello", false, "accept connections but never answer hello")
	flag.BoolVar(&o.Silent, "silent", false, "answer requests but send no data")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	o.Log = log
	s, err := fakegw.Start(*listen, o)
	if err != nil {
		log.Error("listen", "err", err)
		os.Exit(1)
	}
	log.Info("fake gateway listening", "addr", s.Addr())
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	s.Close()
}
