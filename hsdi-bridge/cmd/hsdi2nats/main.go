// Command hsdi2nats connects to one or more HSDI gateways and publishes
// every MIME part they send, unchanged, to a JetStream stream.
//
//	hsdi2nats -gateway ICU=10.0.0.5:2007 -gateway ED=10.0.0.6:2007
//
// See README.md for the subjects, headers and status endpoints.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/UVA-CAMA/physiomon/hsdi-bridge/internal/bridge"
	"github.com/UVA-CAMA/physiomon/hsdi-bridge/internal/natspub"
	"github.com/UVA-CAMA/physiomon/hsdi-bridge/internal/raw"
)

type gatewayFlags []string

func (g *gatewayFlags) String() string     { return strings.Join(*g, ",") }
func (g *gatewayFlags) Set(v string) error { *g = append(*g, v); return nil }

func main() {
	var gateways gatewayFlags
	flag.Var(&gateways, "gateway", "gateway as name=host:port; repeat for more than one")
	natsURL := flag.String("nats", nats.DefaultURL, "NATS server URL")
	stream := flag.String("stream", raw.DefaultStream, "JetStream stream for raw messages")
	createStream := flag.Bool("create-stream", true, "create the stream if it does not exist (an existing stream is not changed)")
	maxAge := flag.Duration("max-age", 7*24*time.Hour, "max age of messages in a newly created stream")
	maxBytes := flag.Int64("max-bytes", -1, "max bytes of a newly created stream; -1 for no limit")
	stateStream := flag.String("state-stream", raw.DefaultStateStream, "JetStream stream for descriptors, session updates and events, kept without an age limit")
	stateMaxBytes := flag.Int64("state-max-bytes", -1, "max bytes of a newly created state stream; -1 for no limit")
	maxPending := flag.Int("max-pending", 4000, "max unacknowledged publishes before reading from gateways waits")
	httpAddr := flag.String("http", "127.0.0.1:8090", "address for /healthz and /status; empty to disable")
	statsEvery := flag.Duration("stats-interval", time.Minute, "how often to log counters; 0 to disable")
	logLevel := flag.String("log-level", "info", "debug, info, warn or error")
	logJSON := flag.Bool("log-json", false, "log as JSON lines")
	var cfg bridge.Config
	flag.DurationVar(&cfg.KeepAliveIdle, "keepalive-idle", 30*time.Second, "send a keep-alive after this long with nothing read")
	flag.DurationVar(&cfg.KeepAliveTimeout, "keepalive-timeout", 10*time.Second, "reconnect if a keep-alive gets no reply in this time")
	flag.DurationVar(&cfg.ReRequestDelay, "rerequest-delay", 5*time.Second, "wait after a binEnd before requesting the stream again; negative to disable")
	flag.DurationVar(&cfg.ResponseTimeout, "response-timeout", 2*time.Second, "log a response as late after this long")
	flag.DurationVar(&cfg.BackoffMax, "backoff-max", time.Minute, "longest wait between reconnect attempts")
	flag.Parse()

	log := newLogger(*logLevel, *logJSON)
	if err := run(log, gateways, *natsURL, *stream, *stateStream, *createStream, *maxAge, *maxBytes, *stateMaxBytes, *maxPending, *httpAddr, *statsEvery, cfg); err != nil {
		log.Error("exiting", "err", err)
		os.Exit(1)
	}
}

func newLogger(level string, asJSON bool) *slog.Logger {
	var l slog.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		l = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: l}
	if asJSON {
		return slog.New(slog.NewJSONHandler(os.Stderr, opts))
	}
	return slog.New(slog.NewTextHandler(os.Stderr, opts))
}

func run(log *slog.Logger, gateways []string, natsURL, stream, stateStream string, createStream bool, maxAge time.Duration,
	maxBytes, stateMaxBytes int64, maxPending int, httpAddr string, statsEvery time.Duration, base bridge.Config) error {
	if len(gateways) == 0 {
		return errors.New("no -gateway given")
	}
	var cfgs []bridge.Config
	seen := map[string]bool{}
	for _, g := range gateways {
		name, addr, ok := strings.Cut(g, "=")
		if !ok || !raw.ValidGatewayName(name) {
			return fmt.Errorf("bad -gateway %q: want name=host:port, name of letters, digits, - and _", g)
		}
		if seen[name] {
			return fmt.Errorf("gateway name %q given twice", name)
		}
		seen[name] = true
		if _, _, err := net.SplitHostPort(addr); err != nil {
			addr = net.JoinHostPort(addr, "2007")
		}
		c := base
		c.Name, c.Addr = name, addr
		cfgs = append(cfgs, c)
	}

	nc, err := nats.Connect(natsURL,
		nats.Name("hsdi2nats"),
		nats.MaxReconnects(-1),
		nats.ReconnectBufSize(64<<20),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) { log.Warn("NATS disconnected", "err", err) }),
		nats.ReconnectHandler(func(c *nats.Conn) { log.Info("NATS reconnected", "url", c.ConnectedUrl()) }))
	if err != nil {
		return fmt.Errorf("connect to NATS: %w", err)
	}
	defer nc.Drain()
	pub, err := natspub.New(nc, maxPending, log)
	if err != nil {
		return err
	}
	if err := ensureStream(pub.JetStream(), raw.StreamConfig(stream, maxAge, maxBytes), createStream, log); err != nil {
		return err
	}
	if err := ensureStream(pub.JetStream(), raw.StateStreamConfig(stateStream, stateMaxBytes), createStream, log); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var bridges []*bridge.Bridge
	var wg sync.WaitGroup
	for _, c := range cfgs {
		b := bridge.New(c, pub, log)
		bridges = append(bridges, b)
		wg.Add(1)
		go func() { defer wg.Done(); b.Run(ctx) }()
		log.Info("gateway configured", "gateway", c.Name, "addr", c.Addr, "subjects", raw.GatewayFilter(c.Name))
	}

	if httpAddr != "" {
		srv := &http.Server{Addr: httpAddr, Handler: statusHandler(bridges, cfgs, pub), ReadHeaderTimeout: 5 * time.Second}
		go func() {
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("status server", "err", err)
			}
		}()
		defer srv.Close()
	}
	if statsEvery > 0 {
		go func() {
			t := time.NewTicker(statsEvery)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					for i, b := range bridges {
						log.Info("counters", "gateway", cfgs[i].Name, "connected", b.Connected(), "stats", b.Stats.Snapshot())
					}
				}
			}
		}()
	}

	<-ctx.Done()
	log.Info("shutting down")
	wg.Wait()
	fctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := pub.Flush(fctx); err != nil {
		log.Warn("not all messages acknowledged before exit", "err", err)
	}
	return nil
}

// ensureStream checks that the stream cfg.Name exists and creates it from cfg
// if not. An existing stream is not changed.
func ensureStream(js jetstream.JetStream, cfg jetstream.StreamConfig, create bool, log *slog.Logger) error {
	name, want := cfg.Name, cfg.Subjects[0]
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := js.Stream(ctx, name)
	if err == nil {
		info := s.CachedInfo()
		covered := false
		for _, subj := range info.Config.Subjects {
			if subj == want || subj == ">" {
				covered = true
			}
		}
		if !covered {
			log.Warn("existing stream may not capture the raw subjects", "stream", name, "subjects", info.Config.Subjects, "want", want)
		}
		return nil
	}
	if !errors.Is(err, jetstream.ErrStreamNotFound) {
		return fmt.Errorf("look up stream %s: %w", name, err)
	}
	if !create {
		return fmt.Errorf("stream %s does not exist and -create-stream=false", name)
	}
	if _, err := js.CreateStream(ctx, cfg); err != nil {
		return fmt.Errorf("create stream %s: %w", name, err)
	}
	log.Info("created stream", "stream", name, "subjects", want, "maxAge", cfg.MaxAge.String())
	return nil
}

func statusHandler(bridges []*bridge.Bridge, cfgs []bridge.Config, pub *natspub.Publisher) http.Handler {
	mux := http.NewServeMux()
	// /healthz is 200 when every gateway is connected and has sent something
	// within its keep-alive window.
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		var bad []string
		for i, b := range bridges {
			window := cfgs[i].KeepAliveIdle + cfgs[i].KeepAliveTimeout + 5*time.Second
			if !b.Connected() || time.Since(b.LastRead()) > window {
				bad = append(bad, cfgs[i].Name)
			}
		}
		if len(bad) > 0 {
			http.Error(w, "not connected: "+strings.Join(bad, ", "), http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
		out := map[string]any{}
		gws := map[string]any{}
		for i, b := range bridges {
			g := map[string]any{
				"addr":      cfgs[i].Addr,
				"connected": b.Connected(),
				"lastRead":  b.LastRead().UTC().Format(time.RFC3339),
				"stats":     b.Stats.Snapshot(),
			}
			if !b.Connected() {
				n, since := b.Down()
				g["failedAttempts"] = n
				g["downSince"] = since.UTC().Format(time.RFC3339)
			}
			gws[cfgs[i].Name] = g
		}
		out["gateways"] = gws
		out["publisher"] = map[string]int64{"stalls": pub.Stalls.Load(), "retries": pub.Retries.Load(), "ackErrors": pub.AckErrors.Load()}
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		enc.Encode(out)
	})
	return mux
}
