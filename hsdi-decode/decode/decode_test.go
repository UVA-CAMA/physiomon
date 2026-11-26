package decode

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const sessionID = "7E146455000007D04CAC9BCD"

// Invented values, in the shape of the real session updates.
const sessionUpdate = `<?xml version="1.0" encoding="UTF-8"?>
<s:sapphire xmlns:s="urn:ge:sapphire:sapphire_1" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" version="2.0.1">
<sessionUpdate xmlns="urn:ge:sapphire:sessionUpdateMsg_1">
<header xmlns="urn:ge:sapphire:base_1"><sessionID V="7EC46464000007D74A3AAC99"/></header>
<sessionList xmlns="urn:ge:sapphire:base_1"><session status="added">
  <sessionID V="` + sessionID + `" type="IP-32:PORT-32:NTPS-32"/>
  <deviceStatus><state><network V="ON_LINE"/><patientStatus V="ADMIT"/></state></deviceStatus>
  <patientInfo><identifier><id V="MRN1"/><primary V="true"/></identifier>
    <visit><assignedLocation xsi:type="ge.locType.Unity"><unitName V="UNIT1"/><bedName V="BED1"/></assignedLocation></visit>
  </patientInfo>
</session></sessionList></sessionUpdate></s:sapphire>`

// Two ECG leads and a heart rate with an episodic structure behind a null
// pointer, in the shapes of real descriptors.
const testDescriptor = `<?xml version="1.0" encoding="UTF-8"?>
<s:sapphire xmlns:s="urn:ge:sapphire:sapphire_1" version="2.0.1"><binDescriptor xmlns="urn:ge:sapphire:binDesc_1">
<header xmlns="urn:ge:sapphire:base_1"><sessionID V="` + sessionID + `"/><msgCHN V="2"/></header>
<block U="Hz" rate="4" repeat="1">
<formatID BT="xs:unsignedInt" iBV="4660"/>
<sessionID BT="xs:unsignedByte" asizeBT="12"/>
<blockSQN BT="xs:unsignedInt" iBV="1"/>
<blockLength BT="xs:unsignedShort"/>
<startDateTime BT="xb:NTP-32" INV="0"/>
<groupBT repeat="1"><params xmlns="urn:ge:sapphire:param_1">
<ecg source="ge:unityDevice" index="1"><wav><ecgWaveformMXG>
<ecgWaveform U="uV" S="2.44" BT="xs:short" INV="-32768 -32767" mapINV="INV MISS" INV_LE="-32752" asizeBT="3" lead="I"/>
<ecgWaveform U="uV" S="2.44" BT="xs:short" INV="-32768 -32767" mapINV="INV MISS" INV_LE="-32752" asizeBT="3" lead="II"/>
</ecgWaveformMXG></wav>
<num><heartRate U="{beat}/min" BT="xs:short" INV="-32768"/></num></ecg>
</params></groupBT></block></binDescriptor></s:sapphire>`

func frame() []byte {
	var b bytes.Buffer
	w := func(v any) { binary.Write(&b, binary.BigEndian, v) }
	sess, _ := hex.DecodeString(sessionID)
	w(uint32(0x40000000 | 4660))
	b.Write(sess)
	w(uint32(7))
	w(uint16(4 + 12 + 4 + 2 + 4 + 2*7))
	w(uint32(3155673600)) // 2000-01-01 in NTP seconds
	w([]int16{1, -2, 3, -32768, 5, 6, 72})
	return b.Bytes()
}

// pb is a minimal protobuf reader: field number to raw values, varints as
// uint64 and length-delimited fields as []byte.
func pb(t *testing.T, b []byte) map[int][]any {
	t.Helper()
	out := map[int][]any{}
	for len(b) > 0 {
		k, n := binary.Uvarint(b)
		b = b[n:]
		f, wire := int(k>>3), k&7
		switch wire {
		case 0:
			v, n := binary.Uvarint(b)
			out[f] = append(out[f], v)
			b = b[n:]
		case 1:
			out[f] = append(out[f], math.Float64frombits(binary.LittleEndian.Uint64(b)))
			b = b[8:]
		case 2:
			l, n := binary.Uvarint(b)
			out[f] = append(out[f], b[n:n+int(l)])
			b = b[n+int(l):]
		default:
			t.Fatalf("wire type %d", wire)
		}
	}
	return out
}

func unzigzag(u uint64) int64 { return int64(u>>1) ^ -int64(u&1) }

func TestFrame(t *testing.T) {
	d := New()
	if err := d.SessionUpdate("GW", []byte(sessionUpdate)); err != nil {
		t.Fatal(err)
	}
	if err := d.Descriptor("GW", []byte(testDescriptor)); err != nil {
		t.Fatal(err)
	}
	recv := time.Date(2026, 1, 2, 3, 4, 5, 6e6, time.UTC)
	f, err := d.Frame("GW", frame(), recv, 99)
	if err != nil || f == nil {
		t.Fatalf("frame %v, %v", f, err)
	}
	if f.Service != "wav" || f.FormatID != 4660 || f.BlockSQN != 7 || f.StartTime != 946684800 ||
		f.Patient == nil || f.Patient.PatientID != "MRN1" || f.Patient.Unit != "UNIT1" || len(f.Signals) != 3 {
		t.Fatalf("%+v", f)
	}

	m := pb(t, Encode(f))
	if string(m[1][0].([]byte)) != "GW" || string(m[2][0].([]byte)) != sessionID || m[4][0] != uint64(4660) ||
		m[6][0] != uint64(946684800) || m[7][0] != uint64(recv.UnixMilli()) || m[8][0] != 4.0 || m[9][0] != uint64(99) {
		t.Errorf("frame fields %v", m)
	}
	p := pb(t, m[3][0].([]byte))
	if string(p[1][0].([]byte)) != "MRN1" || string(p[3][0].([]byte)) != "UNIT1" || string(p[4][0].([]byte)) != "BED1" {
		t.Errorf("patient %v", p)
	}
	var names []string
	for _, raw := range m[10] {
		s := pb(t, raw.([]byte))
		names = append(names, string(s[1][0].([]byte)))
	}
	if want := "ecg/wav/ecgWaveformMXG/ecgWaveform lead=I index=1|ecg/wav/ecgWaveformMXG/ecgWaveform lead=II index=1|ecg/num/heartRate index=1"; strings.Join(names, "|") != want {
		t.Errorf("names %q", names)
	}

	lead2 := pb(t, m[10][1].([]byte))
	samples := lead2[9][0].([]byte)
	if len(samples) != 6 || int16(binary.LittleEndian.Uint16(samples[0:])) != -32768 || int16(binary.LittleEndian.Uint16(samples[4:])) != 6 {
		t.Errorf("lead II samples % x", samples)
	}
	if lead2[4][0] != 2.44 || string(lead2[3][0].([]byte)) != "uV" {
		t.Errorf("lead II scale or unit %v", lead2)
	}
	var inv []int64
	for b := lead2[11][0].([]byte); len(b) > 0; {
		u, n := binary.Uvarint(b)
		inv, b = append(inv, unzigzag(u)), b[n:]
	}
	if len(inv) != 2 || inv[0] != -32768 || inv[1] != -32767 || string(lead2[12][1].([]byte)) != "MISS" ||
		unzigzag(lead2[13][0].(uint64)) != -32752 {
		t.Errorf("invalid %v %v %v", inv, lead2[12], lead2[13])
	}
	attrs := map[string]string{}
	for _, e := range lead2[2] {
		kv := pb(t, e.([]byte))
		attrs[string(kv[1][0].([]byte))] = string(kv[2][0].([]byte))
	}
	if attrs["lead"] != "II" || attrs["index"] != "1" || attrs["source"] != "ge:unityDevice" {
		t.Errorf("attrs %v", attrs)
	}

	hr := pb(t, m[10][2].([]byte))
	if v, _ := binary.Uvarint(hr[7][0].([]byte)); unzigzag(v) != 72 {
		t.Errorf("heart rate %v", hr[7])
	}

	// A frame without a descriptor, and one too short for it.
	other := frame()
	other[3] = 1
	if f, err := d.Frame("GW", other, recv, 100); f != nil || err != nil {
		t.Errorf("no descriptor: %v %v", f, err)
	}
	if _, err := d.Frame("GW", frame()[:30], recv, 101); err == nil {
		t.Error("short frame decoded")
	}
	if d.Stats.Frames != 1 || d.Stats.NoDescriptor != 1 || d.Stats.DecodeErrors != 1 {
		t.Errorf("stats %+v", d.Stats)
	}

	// Removing the session drops its descriptor.
	removed := strings.Replace(sessionUpdate, `status="added"`, `status="removed"`, 1)
	if err := d.SessionUpdate("GW", []byte(removed)); err != nil {
		t.Fatal(err)
	}
	if f, _ := d.Frame("GW", frame(), recv, 102); f != nil || d.Stats.NoDescriptor != 2 {
		t.Errorf("descriptor kept after the session was removed")
	}
}

func testLog() *slog.Logger {
	if os.Getenv("HSDI_TEST_LOG") != "" {
		return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// env is a NATS server with the raw, state and decoded streams, and a
// publisher of raw-stream messages in the form hsdi2nats writes them.
type env struct {
	t   *testing.T
	nc  *nats.Conn
	js  jetstream.JetStream
	now time.Time
	sqn uint32
}

func newEnv(t *testing.T) *env {
	ns, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats not ready")
	}
	t.Cleanup(ns.Shutdown)
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	for _, cfg := range []jetstream.StreamConfig{
		{Name: RawStream, Subjects: []string{rawPrefix + ".>"}},
		{Name: StateStream, Subjects: []string{statePrefix + ".>"}},
		StreamConfig(DefaultStream, time.Hour, -1),
	} {
		if _, err := js.CreateStream(context.Background(), cfg); err != nil {
			t.Fatal(err)
		}
	}
	return &env{t: t, nc: nc, js: js, now: time.Now().Add(-time.Hour)}
}

// publish writes one raw message, unless typ is empty, and its state stream
// copy if state is not empty, a millisecond after the previous one.
func (e *env) publish(typ, state string, data []byte) {
	e.t.Helper()
	e.now = e.now.Add(time.Millisecond)
	var subjects []string
	if typ != "" {
		subjects = append(subjects, rawSubject("GW", typ))
	}
	if state != "" {
		subjects = append(subjects, statePrefix+".GW."+state)
	}
	for _, subj := range subjects {
		m := nats.NewMsg(subj)
		m.Data = data
		m.Header.Set(headerReceived, e.now.UTC().Format(time.RFC3339Nano))
		if _, err := e.js.PublishMsg(context.Background(), m); err != nil {
			e.t.Fatal(err)
		}
	}
}

func (e *env) patient(mrn string) {
	e.publish("sessionUpdate", "sessionUpdate", []byte(strings.Replace(sessionUpdate, "MRN1", mrn, 1)))
}

func (e *env) frames(n int) {
	for range n {
		e.sqn++
		f := frame()
		binary.BigEndian.PutUint32(f[16:], e.sqn)
		e.publish(typeBinary, "", f)
	}
}

// run runs the decoder until the decoded stream holds want messages.
func (e *env) run(consumer, start string, want int) {
	e.t.Helper()
	cfg := Config{RawStream: RawStream, StateStream: StateStream, Consumer: consumer, Start: start, Batch: 7}
	sink, err := NewNATSSink(e.nc, 8)
	if err != nil {
		e.t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, e.nc, cfg, sink, testLog()) }()
	waitFor(e.t, fmt.Sprintf("%d decoded frames", want), func() bool { return count(e.t, e.js) >= want })
	stop()
	if err := <-done; err != nil {
		e.t.Fatal(err)
	}
}

// decoded returns the patient ID of each decoded frame, by block sequence
// number, and fails on a frame decoded twice.
func (e *env) decoded() map[uint64]string {
	e.t.Helper()
	cons, err := e.js.OrderedConsumer(context.Background(), DefaultStream, jetstream.OrderedConsumerConfig{})
	if err != nil {
		e.t.Fatal(err)
	}
	n := count(e.t, e.js)
	out := map[uint64]string{}
	seen := map[uint64]bool{}
	for got := 0; got < n; {
		batch, err := cons.Fetch(n-got, jetstream.FetchMaxWait(2*time.Second))
		if err != nil {
			e.t.Fatal(err)
		}
		for m := range batch.Messages() {
			got++
			f := pb(e.t, m.Data())
			if seq := f[9][0].(uint64); seen[seq] {
				e.t.Fatalf("raw sequence %d decoded twice", seq)
			} else {
				seen[seq] = true
			}
			mrn := ""
			if len(f[3]) > 0 {
				mrn = string(pb(e.t, f[3][0].([]byte))[1][0].([]byte))
			}
			out[f[5][0].(uint64)] = mrn
		}
	}
	return out
}

// The decoder starts after the session update and descriptor were sent, so
// it must take them from the state stream. It is stopped and started again
// across a change of patient, and must neither repeat nor skip frames, and
// give each frame the patient in force when it arrived.
func TestEndToEnd(t *testing.T) {
	e := newEnv(t)
	e.patient("MRN1")
	e.publish("binDescriptor", "binDescriptor."+sessionID+".2", []byte(testDescriptor))
	e.frames(5) // before the decoder exists: not decoded

	go func() { time.Sleep(300 * time.Millisecond); e.frames(10) }()
	e.run(DefaultConsumer, "new", 10)
	e.patient("MRN2")
	e.frames(10)
	e.run(DefaultConsumer, "new", 20)

	got := e.decoded()
	if len(got) != 20 {
		t.Fatalf("%d frames decoded, want 20", len(got))
	}
	for sqn := uint64(6); sqn <= 25; sqn++ {
		want := "MRN1"
		if sqn > 15 {
			want = "MRN2"
		}
		if got[sqn] != want {
			t.Errorf("frame %d: patient %q, want %q", sqn, got[sqn], want)
		}
	}
}

// The first session update and the descriptor are only in the state stream,
// as when the raw stream no longer holds them. A later session update, in
// both streams, must not be applied to the frames before it.
func TestStateCutoff(t *testing.T) {
	e := newEnv(t)
	e.publish("", "sessionUpdate", []byte(sessionUpdate))
	e.publish("", "binDescriptor."+sessionID+".2", []byte(testDescriptor))
	e.frames(5)
	e.patient("MRN2")
	e.frames(5)
	e.run(DefaultConsumer, "all", 10)
	got := e.decoded()
	for sqn := uint64(1); sqn <= 10; sqn++ {
		want := "MRN1"
		if sqn > 5 {
			want = "MRN2"
		}
		if got[sqn] != want {
			t.Errorf("frame %d: patient %q, want %q", sqn, got[sqn], want)
		}
	}
}

func count(t *testing.T, js jetstream.JetStream) int {
	t.Helper()
	s, err := js.Stream(context.Background(), DefaultStream)
	if err != nil {
		t.Fatal(err)
	}
	info, err := s.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return int(info.State.Msgs)
}

func waitFor(t *testing.T, what string, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !f() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
