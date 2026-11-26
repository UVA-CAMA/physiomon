// Command hsdi-report reads the raw stream and prints a structure report:
// message counts and sizes, MIME header line counts, bridge events, frame
// counts and rates per channel, every distinct descriptor shape, and the
// element paths of other XML messages. Values that could identify a patient
// or carry a gateway date are withheld; see internal/report.
//
// The report is meant to be reviewed by a person inside the PHI boundary
// before it is shared.
//
//	hsdi-report -gateway ICU -since 24h > icu-report.txt
package main

import (
	"cmp"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/UVA-CAMA/physiomon/hsdi-bridge/internal/raw"
	"github.com/UVA-CAMA/physiomon/hsdi-bridge/internal/report"
)

func main() {
	natsURL := flag.String("nats", nats.DefaultURL, "NATS server URL")
	stream := flag.String("stream", raw.DefaultStream, "raw stream")
	stateStream := flag.String("state-stream", raw.DefaultStateStream, "state stream, read in full for descriptors; empty to use only descriptors in the raw window")
	gateway := flag.String("gateway", "", "gateway name; empty for all gateways")
	since := flag.Duration("since", 0, "read raw messages received in this period before now; 0 for the whole stream")
	limit := flag.Int("limit", 0, "stop after this many raw messages; 0 for no limit")
	labels := flag.Bool("labels", true, "show parameter labels in descriptors (usually set by staff, manual 4-4)")
	discarded := flag.Bool("discarded", false, "instead of the report, print the bytes the bridge skipped, as received. "+
		"This is raw gateway content and may contain PHI; it is for inspection inside the PHI boundary only")
	flag.Parse()
	if *gateway != "" && !raw.ValidGatewayName(*gateway) {
		fmt.Fprintln(os.Stderr, "hsdi-report: bad gateway name")
		os.Exit(2)
	}
	if err := run(*natsURL, *stream, *stateStream, *gateway, *since, *limit, *labels, *discarded); err != nil {
		fmt.Fprintln(os.Stderr, "hsdi-report:", err)
		os.Exit(1)
	}
}

func run(url, stream, stateStream, gateway string, since time.Duration, limit int, labels, dumpDiscarded bool) error {
	nc, err := nats.Connect(url, nats.Name("hsdi-report"))
	if err != nil {
		return err
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	ctx := context.Background()
	c := report.NewCollector(labels)

	if dumpDiscarded {
		cfg := rawConfig("", since)
		cfg.FilterSubjects = []string{raw.Subject(cmp.Or(gateway, "*"), raw.DirIn, raw.TypeDiscarded)}
		return read(ctx, js, stream, cfg, 0, func(m jetstream.Msg) {
			md, _ := m.Metadata()
			n, _ := raw.ParseSubject(m.Subject())
			fmt.Printf("=== stream seq %d, %s %s, %s bytes skipped before the next message\n",
				md.Sequence.Stream, n.Gateway, m.Headers().Get(raw.HReceived), m.Headers().Get(raw.HDiscarded))
			os.Stdout.Write(m.Data())
			fmt.Println()
		})
	}

	if stateStream != "" {
		filter := raw.StatePrefix + ".*.binDescriptor.>"
		if gateway != "" {
			filter = raw.StatePrefix + "." + gateway + ".binDescriptor.>"
		}
		cfg := jetstream.OrderedConsumerConfig{FilterSubjects: []string{filter}}
		err := read(ctx, js, stateStream, cfg, 0, func(m jetstream.Msg) { c.AddState(m.Data()) })
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			fmt.Fprintf(os.Stderr, "state stream %s not found; using only descriptors in the raw window\n", stateStream)
		} else if err != nil {
			return fmt.Errorf("state stream: %w", err)
		}
	}

	if err := read(ctx, js, stream, rawConfig(gateway, since), limit, func(m jetstream.Msg) { c.Add(m.Subject(), m.Headers(), m.Data()) }); err != nil {
		return err
	}
	c.Write(os.Stdout)
	return nil
}

func rawConfig(gateway string, since time.Duration) jetstream.OrderedConsumerConfig {
	cfg := jetstream.OrderedConsumerConfig{}
	if gateway != "" {
		cfg.FilterSubjects = []string{raw.GatewayFilter(gateway)}
	}
	if since > 0 {
		t := time.Now().Add(-since)
		cfg.DeliverPolicy = jetstream.DeliverByStartTimePolicy
		cfg.OptStartTime = &t
	}
	return cfg
}

// firstWait is how long read waits for the first message. Finding the start
// of a time window in a large stream can take longer than one fetch.
const firstWait = time.Minute

// read passes each message of stream that matches cfg to f, up to limit
// messages if limit is positive, and returns when it has caught up.
func read(ctx context.Context, js jetstream.JetStream, stream string, cfg jetstream.OrderedConsumerConfig, limit int, f func(jetstream.Msg)) error {
	s, err := js.Stream(ctx, stream)
	if err != nil {
		return err
	}
	cons, err := s.OrderedConsumer(ctx, cfg)
	if err != nil {
		return err
	}
	n := 0
	start := time.Now()
	for {
		batch, err := cons.Fetch(500, jetstream.FetchMaxWait(2*time.Second))
		if err != nil {
			return err
		}
		got, done := 0, false
		for m := range batch.Messages() {
			got++
			n++
			f(m)
			if md, err := m.Metadata(); err == nil && md.NumPending == 0 {
				done = true
			}
			if limit > 0 && n >= limit {
				done = true
			}
		}
		if err := batch.Error(); err != nil && !errors.Is(err, nats.ErrTimeout) {
			return err
		}
		if n > 0 && n%100000 < got {
			fmt.Fprintf(os.Stderr, "%s: read %d messages in %s\n", stream, n, time.Since(start).Round(time.Second))
		}
		if done || (got == 0 && (n > 0 || time.Since(start) > firstWait)) {
			return nil
		}
	}
}
