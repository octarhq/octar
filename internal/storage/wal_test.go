package storage

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"
)

// ── Helpers ───────────────────────────────────────────────────────────────────

func testEvent(typ EventType, ns, queue, group, msgID string, payload []byte) Event {
	return Event{
		Type:      typ,
		Namespace: ns,
		Queue:     queue,
		Group:     group,
		MsgID:     msgID,
		Payload:   payload,
	}
}

func walConfig() WALConfig {
	return WALConfig{
		FlushInterval:    50 * time.Millisecond,
		FlushMaxMessages: 100,
		SegmentMaxBytes:  256 * 1024,
		Durable:          true,
	}
}

// readAllEvents reads every event from all .log files in dir, sorted by segment ID.
func readAllEvents(t testing.TB, dir string) []Event {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var logFiles []string
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".log" {
			logFiles = append(logFiles, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(logFiles)
	var events []Event
	for _, path := range logFiles {
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		r := bufio.NewReader(f)
		for {
			e, err := ReadEvent(r)
			if err == io.EOF {
				break
			}
			if err != nil {
				f.Close()
				t.Fatalf("ReadEvent %s: %v", path, err)
			}
			events = append(events, e)
		}
		f.Close()
	}
	return events
}

func countLogFiles(t testing.TB, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var n int
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".log" {
			n++
		}
	}
	return n
}

// failingWriter simulates flush errors after N successful flushes.
type failingWriter struct {
	buf       bytes.Buffer
	flushes   int
	failAfter int
	failErr   error
}

func newFailingWriter(after int, err error) *failingWriter {
	return &failingWriter{failAfter: after, failErr: err}
}

func (w *failingWriter) Write(p []byte) (int, error)       { return w.buf.Write(p) }
func (w *failingWriter) WriteString(s string) (int, error) { return w.buf.WriteString(s) }
func (w *failingWriter) Flush() error {
	w.flushes++
	if w.flushes > w.failAfter {
		return w.failErr
	}
	return nil
}

var errDiskFull = errors.New("no space left on device")

// safeClose recovers from double-close panics so it can be used in t.Cleanup
// even when the test body already called Close explicitly.
func safeClose(w *WAL) {
	defer func() { _ = recover() }()
	w.Close()
}

// eventSize returns the on-disk binary size of an event for a given payload length.
// Useful for calculating SegmentMaxBytes thresholds.
func eventSize(payloadLen int) int {
	return 21 + 2 + 2 + 2 + 1 + 2 + 1 + 2 + 1 + 2 + 4 + 4 + payloadLen + 4
}

// ═══════════════════════════════════════════════════════════════════════════════
// 1. AppendSync: publish event, checks .log created, valid binary content
// ═══════════════════════════════════════════════════════════════════════════════

func TestWAL_AppendSync(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWAL(dir, walConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { safeClose(w) })

	e := testEvent(EventPublish, "ns1", "q1", "g1", "msg-1", []byte("hello world"))
	if err := w.AppendSync(e); err != nil {
		t.Fatal(err)
	}
	w.Close()

	qw := w.GetQueue("ns1", "q1")
	if qw == nil {
		t.Fatal("queueWAL not found after AppendSync")
	}

	if n := countLogFiles(t, qw.dir); n != 1 {
		t.Fatalf("expected 1 .log file, got %d", n)
	}

	events := readAllEvents(t, qw.dir)
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	got := events[0]
	if got.Type != EventPublish {
		t.Fatalf("Type=%d, want %d", got.Type, EventPublish)
	}
	if got.Namespace != "ns1" || got.Queue != "q1" || got.Group != "g1" || got.MsgID != "msg-1" {
		t.Fatalf("fields mismatch: %+v", got)
	}
	if string(got.Payload) != "hello world" {
		t.Fatalf("Payload=%q, want %q", got.Payload, "hello world")
	}
	if got.Seq == 0 {
		t.Error("Seq is zero, expected auto-assigned sequence number")
	}
	if got.Timestamp == 0 {
		t.Error("Timestamp is zero, expected auto-assigned timestamp")
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// 2. Append: LEASE non-blocking, verify in .log
// ═══════════════════════════════════════════════════════════════════════════════

func TestWAL_Append(t *testing.T) {
	dir := t.TempDir()
	cfg := walConfig()
	cfg.FlushInterval = time.Hour // never flush by timer; we rely on Close to drain
	w, err := NewWAL(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}

	e := testEvent(EventLease, "ns2", "q2", "g2", "lease-1", nil)
	if err := w.Append(e); err != nil {
		w.Close()
		t.Fatal(err)
	}
	w.Close() // flush everything

	qw := w.GetQueue("ns2", "q2")
	events := readAllEvents(t, qw.dir)
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Type != EventLease {
		t.Fatalf("Type=%d, want %d", events[0].Type, EventLease)
	}
	if events[0].Seq == 0 {
		t.Error("Seq is zero")
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// 3. Batch flush: flush_max_messages=5, sends 5 events, verifies flush
// ═══════════════════════════════════════════════════════════════════════════════
//
// We send the first 4 via Append (non-blocking) so they accumulate in the
// writer's batch, then the 5th via AppendSync which triggers the flush.
// This ensures the flush is fired by the threshold, not the timer.

func TestWAL_BatchFlush(t *testing.T) {
	dir := t.TempDir()
	cfg := walConfig()
	cfg.FlushMaxMessages = 5
	cfg.FlushInterval = time.Hour // timer must NOT trigger; flush by count only
	w, err := NewWAL(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { safeClose(w) })

	// first 4 — non-blocking, accumulate in the batch
	for i := range 4 {
		e := testEvent(EventPublish, "ns", "q", "g",
			fmt.Sprintf("msg-%d", i),
			[]byte(fmt.Sprintf("payload-%d", i)))
		if err := w.Append(e); err != nil {
			t.Fatalf("Append(%d): %v", i, err)
		}
	}

	// 5th event — triggers the flush
	e5 := testEvent(EventPublish, "ns", "q", "g", "msg-4", []byte("payload-4"))
	if err := w.AppendSync(e5); err != nil {
		t.Fatal(err)
	}
	w.Close()

	qw := w.GetQueue("ns", "q")
	events := readAllEvents(t, qw.dir)
	if len(events) != 5 {
		t.Fatalf("expected 5 events, got %d", len(events))
	}
	for i, ev := range events {
		want := fmt.Sprintf("payload-%d", i)
		if string(ev.Payload) != want {
			t.Errorf("events[%d] Payload=%q, want %q", i, string(ev.Payload), want)
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// 4. Segment rotation: small SegmentMaxBytes, writes until rotation
// ═══════════════════════════════════════════════════════════════════════════════
//
// eventSize overhead ≈ 48 bytes (exact depends on field lengths). With 50-byte
// payload each event is ~98 bytes. SegmentMaxBytes=800 holds ~8 events.
// 15 events produce 2 segments; snapshot cleanup keeps both (keepFrom = 0).

func TestWAL_SegmentRotation(t *testing.T) {
	dir := t.TempDir()
	cfg := walConfig()
	cfg.SegmentMaxBytes = int64(eventSize(50))*8 + 1 // ~785 bytes, 8 events per segment
	cfg.FlushMaxMessages = 1
	w, err := NewWAL(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { safeClose(w) })

	payload := make([]byte, 50)
	for i := range 15 {
		e := testEvent(EventPublish, "ns", "q", "g",
			fmt.Sprintf("msg-%d", i), payload)
		if err := w.AppendSync(e); err != nil {
			t.Fatalf("event %d: %v", i, err)
		}
	}
	w.Close()

	qw := w.GetQueue("ns", "q")
	if n := countLogFiles(t, qw.dir); n < 2 {
		t.Fatalf("expected >=2 .log files after rotation, got %d", n)
	}

	// After only one rotation the snapshot cleanup keeps the first segment
	// (keepFrom = 1-1 = 0), so all 15 events survive.
	events := readAllEvents(t, qw.dir)
	if len(events) != 15 {
		t.Fatalf("expected 15 events across all segments, got %d", len(events))
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// 5. rotateSegment atomic: new segment exists before old one is closed
// ═══════════════════════════════════════════════════════════════════════════════
//
// flushBatch calls signalDone BEFORE rotateSegment, so after AppendSync
// returns the rotation might not be complete yet. We synchronise by acquiring
// qw.mu: the writer goroutine holds qw.mu during flushBatch (including
// rotation), so acquiring the lock after AppendSync means rotation is done.

func TestWAL_RotateSegmentAtomic(t *testing.T) {
	dir := t.TempDir()
	cfg := walConfig()
	overhead := eventSize(0)
	// Use a payload that makes one event just under the limit, two events over it.
	maxPerSeg := overhead + 10
	payload := make([]byte, maxPerSeg-overhead-1) // event fits exactly in one segment
	cfg.SegmentMaxBytes = int64(eventSize(len(payload))) + 1
	cfg.FlushMaxMessages = 1
	w, err := NewWAL(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { safeClose(w) })

	qw := w.getQueueWAL("ns", "q")

	// first event — fits, no rotation
	e1 := testEvent(EventPublish, "ns", "q", "g", "m1", payload)
	if err := w.AppendSync(e1); err != nil {
		t.Fatal(err)
	}

	// second event triggers rotation
	e2 := testEvent(EventPublish, "ns", "q", "g", "m2", payload)
	if err := w.AppendSync(e2); err != nil {
		t.Fatal(err)
	}

	// synchronise: wait for rotation by acquiring qw.mu
	// (writer loop releases it only after flushBatch+rotation completes)
	qw.mu.Lock()
	segID := qw.segmentID
	qw.mu.Unlock()

	firstSegID, secondSegID := segID-1, segID

	// both segments must exist on disk
	oldPath := filepath.Join(qw.dir, fmt.Sprintf("%09d.log", firstSegID))
	newPath := filepath.Join(qw.dir, fmt.Sprintf("%09d.log", secondSegID))
	if _, err := os.Stat(oldPath); err != nil {
		t.Fatalf("old segment %s missing after rotation: %v", oldPath, err)
	}
	if _, err := os.Stat(newPath); err != nil {
		t.Fatalf("new segment %s missing after rotation: %v", newPath, err)
	}

	// events written after rotation go to the new segment
	e3 := testEvent(EventPublish, "ns", "q", "g", "m3", []byte("post-rotation"))
	if err := w.AppendSync(e3); err != nil {
		t.Fatal(err)
	}
	w.Close()

	events := readAllEvents(t, qw.dir)
	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}
	if events[0].MsgID != "m1" || events[1].MsgID != "m2" || events[2].MsgID != "m3" {
		t.Fatal("event order or content incorrect after rotation")
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// 6. WAL erro: setErr + Append → error returned
// ═══════════════════════════════════════════════════════════════════════════════

func TestWAL_ErrorAppend(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWAL(dir, walConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { safeClose(w) })

	qw := w.getQueueWAL("ns", "q")
	qw.SetErr(ErrWALFailed)

	e := testEvent(EventLease, "ns", "q", "g", "m1", nil)
	if err := w.Append(e); err == nil {
		t.Fatal("expected error from Append after setErr")
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// 7. WAL erro + AppendSync: error returned
// ═══════════════════════════════════════════════════════════════════════════════

func TestWAL_ErrorAppendSync(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWAL(dir, walConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { safeClose(w) })

	qw := w.getQueueWAL("ns", "q")
	qw.SetErr(ErrWALFailed)

	e := testEvent(EventPublish, "ns", "q", "g", "m1", []byte("x"))
	if err := w.AppendSync(e); err == nil {
		t.Fatal("expected error from AppendSync after setErr")
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// 8. Disco cheio simulado: writer loop detects ENOSPC, setErr é chamado
// ═══════════════════════════════════════════════════════════════════════════════

func TestWAL_DiskFull(t *testing.T) {
	dir := t.TempDir()
	cfg := walConfig()
	cfg.FlushMaxMessages = 1
	cfg.Durable = false
	cfg.FlushInterval = time.Hour
	w, err := NewWAL(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { safeClose(w) })

	qw := w.getQueueWAL("ns", "q")

	// hijack the log buffer with a writer that fails after 1 flush
	fw := newFailingWriter(1, errDiskFull)
	qw.logBuf = fw

	// first event — flush succeeds
	e1 := testEvent(EventPublish, "ns", "q", "g", "m1", []byte("ok"))
	if err := w.AppendSync(e1); err != nil {
		t.Fatalf("expected first event to succeed, got: %v", err)
	}

	// second event — flush fails with ENOSPC → setErr
	e2 := testEvent(EventPublish, "ns", "q", "g", "m2", []byte("fail"))
	if err := w.AppendSync(e2); err == nil {
		t.Fatal("expected error on disk full")
	}

	if qw.Err() == nil {
		t.Fatal("expected QueueWAL to be in error state after disk full")
	}
	if !isDiskFull(qw.Err()) {
		t.Fatalf("expected disk-full error, got: %v", qw.Err())
	}

	// further operations must fail immediately
	e3 := testEvent(EventPublish, "ns", "q", "g", "m3", []byte("also fail"))
	if err := w.AppendSync(e3); err == nil {
		t.Fatal("expected error on third event after disk full")
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// 9. ReadEvent roundtrip: escreve evento, lê de volta, verifica campos + CRC
// ═══════════════════════════════════════════════════════════════════════════════
//
// Each event goes to its own queue. We read from every queue directory.

func TestReadEvent_Roundtrip(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWAL(dir, walConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { safeClose(w) })

	events := []Event{
		testEvent(EventPublish, "ns-a", "q-a", "g-a", "mid-a", []byte("alpha")),
		testEvent(EventLease, "ns-b", "q-b", "g-b", "mid-b", []byte("beta")),
		testEvent(EventACK, "ns-c", "q-c", "g-c", "mid-c", []byte("gamma")),
		testEvent(EventNACK, "ns-d", "q-d", "g-d", "mid-d", []byte("delta")),
		testEvent(EventExpire, "ns-e", "q-e", "g-e", "mid-e", []byte("epsilon")),
	}

	for _, e := range events {
		if err := w.AppendSync(e); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	// collect events from every queue directory
	var got []Event
	for _, e := range events {
		qw := w.GetQueue(e.Namespace, e.Queue)
		if qw == nil {
			t.Fatalf("queueWAL not found for %s/%s", e.Namespace, e.Queue)
		}
		got = append(got, readAllEvents(t, qw.dir)...)
	}

	if len(got) != 5 {
		t.Fatalf("expected 5 events total, got %d", len(got))
	}
	for i, g := range got {
		want := events[i]
		if g.Type != want.Type {
			t.Errorf("[%d] Type=%d, want %d", i, g.Type, want.Type)
		}
		if g.Namespace != want.Namespace || g.Queue != want.Queue ||
			g.Group != want.Group || g.MsgID != want.MsgID {
			t.Errorf("[%d] fields mismatch: got %+v, want %+v", i, g, want)
		}
		if string(g.Payload) != string(want.Payload) {
			t.Errorf("[%d] Payload=%q, want %q", i, string(g.Payload), string(want.Payload))
		}
		if g.Seq == 0 {
			t.Errorf("[%d] Seq is zero", i)
		}
		if g.Timestamp == 0 {
			t.Errorf("[%d] Timestamp is zero", i)
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// 10. ReadEvent CRC mismatch: corrompe payload, ReadEvent retorna erro
// ═══════════════════════════════════════════════════════════════════════════════

func TestReadEvent_CRCMismatch(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWAL(dir, walConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { safeClose(w) })

	e := testEvent(EventPublish, "ns", "q", "g", "m1", []byte("corrupt-me"))
	if err := w.AppendSync(e); err != nil {
		t.Fatal(err)
	}
	w.Close()

	qw := w.GetQueue("ns", "q")
	qw.mu.Lock()
	logFile := filepath.Join(qw.dir, fmt.Sprintf("%09d.log", qw.segmentID))
	qw.mu.Unlock()

	raw, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatal(err)
	}

	idx := bytes.Index(raw, []byte("corrupt-me"))
	if idx < 0 {
		t.Fatal("payload bytes not found in raw log file")
	}
	raw[idx] ^= 0xFF

	if err := os.WriteFile(logFile, raw, 0644); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(logFile)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	_, err = ReadEvent(bufio.NewReader(f))
	if err == nil {
		t.Fatal("expected CRC mismatch error, got nil")
	}
	if !errors.Is(err, ErrCorruptedRecord) {
		t.Fatalf("expected ErrCorruptedRecord, got: %v", err)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// 11. Múltiplos eventos em sequência: 100 eventos, lê todos de volta
// ═══════════════════════════════════════════════════════════════════════════════

func TestWAL_MultipleEvents(t *testing.T) {
	dir := t.TempDir()
	cfg := walConfig()
	cfg.FlushMaxMessages = 10 // flush every 10 events
	w, err := NewWAL(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { safeClose(w) })

	const n = 100
	for i := range n {
		e := testEvent(EventPublish, "ns", "q", "g",
			fmt.Sprintf("msg-%d", i),
			[]byte(fmt.Sprintf("Hello World Event #%d!", i)))
		if i%2 == 0 {
			if err := w.AppendSync(e); err != nil {
				t.Fatalf("AppendSync(%d): %v", i, err)
			}
		} else {
			if err := w.Append(e); err != nil {
				t.Fatalf("Append(%d): %v", i, err)
			}
		}
	}
	w.Close()

	qw := w.GetQueue("ns", "q")
	got := readAllEvents(t, qw.dir)
	if len(got) != n {
		t.Fatalf("expected %d events, got %d", n, len(got))
	}
	for i, ev := range got {
		if ev.MsgID != fmt.Sprintf("msg-%d", i) {
			t.Errorf("[%d] MsgID=%q, want %q", i, ev.MsgID, fmt.Sprintf("msg-%d", i))
		}
		if ev.Type != EventPublish {
			t.Errorf("[%d] Type=%d, want %d", i, ev.Type, EventPublish)
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// 12. Payload grande: 1MB payload, escreve e lê
// ═══════════════════════════════════════════════════════════════════════════════

func TestWAL_LargePayload(t *testing.T) {
	dir := t.TempDir()
	cfg := walConfig()
	cfg.SegmentMaxBytes = 2 * 1024 * 1024 // 2MB to hold the large event
	w, err := NewWAL(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { safeClose(w) })

	payload := make([]byte, 1024*1024) // 1 MB
	for i := range payload {
		payload[i] = byte(i)
	}

	e := testEvent(EventPublish, "ns", "q", "g", "big-msg", payload)
	if err := w.AppendSync(e); err != nil {
		t.Fatal(err)
	}
	w.Close()

	qw := w.GetQueue("ns", "q")
	got := readAllEvents(t, qw.dir)
	if len(got) != 1 {
		t.Fatalf("expected 1 event, got %d", len(got))
	}
	if len(got[0].Payload) != len(payload) {
		t.Fatalf("payload length: got %d, want %d", len(got[0].Payload), len(payload))
	}
	for i, b := range got[0].Payload {
		if b != byte(i) {
			t.Fatalf("payload corrupted at byte %d: got %02x, want %02x", i, b, byte(i))
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// 13. Payload zero: evento sem payload
// ═══════════════════════════════════════════════════════════════════════════════

func TestWAL_ZeroPayload(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWAL(dir, walConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { safeClose(w) })

	e1 := testEvent(EventPublish, "ns", "q", "g", "nil-pl", nil)
	if err := w.AppendSync(e1); err != nil {
		t.Fatal(err)
	}
	e2 := testEvent(EventLease, "ns", "q", "g", "empty-pl", []byte{})
	if err := w.AppendSync(e2); err != nil {
		t.Fatal(err)
	}

	w.Close()

	qw := w.GetQueue("ns", "q")
	got := readAllEvents(t, qw.dir)
	if len(got) != 2 {
		t.Fatalf("expected 2 events, got %d", len(got))
	}
	if len(got[0].Payload) != 0 {
		t.Fatalf("event[0] Payload=%v (len=%d), want nil or empty", got[0].Payload, len(got[0].Payload))
	}
	if len(got[1].Payload) != 0 {
		t.Fatalf("event[1] Payload=%v (len=%d), want nil or empty", got[1].Payload, len(got[1].Payload))
	}
	if got[0].Seq == 0 || got[1].Seq == 0 {
		t.Error("Seq is zero for zero-payload event")
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// 14. DestroyQueue: cria queueWAL, escreve dados, DestroyQueue remove tudo
// ═══════════════════════════════════════════════════════════════════════════════

func TestWAL_DestroyQueue(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWAL(dir, walConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { safeClose(w) })

	for i := range 5 {
		e := testEvent(EventPublish, "ns", "q-a", "g", fmt.Sprintf("m-%d", i), []byte("a"))
		_ = w.AppendSync(e)
		e = testEvent(EventPublish, "ns", "q-b", "g", fmt.Sprintf("m-%d", i), []byte("b"))
		_ = w.AppendSync(e)
	}

	qwa := w.GetQueue("ns", "q-a")
	qwb := w.GetQueue("ns", "q-b")
	dirA := qwa.dir
	dirB := qwb.dir

	if _, err := os.Stat(dirA); err != nil {
		t.Fatalf("q-a dir should exist before destroy: %v", err)
	}
	if _, err := os.Stat(dirB); err != nil {
		t.Fatalf("q-b dir should exist before destroy: %v", err)
	}

	if err := w.DestroyQueue("ns", "q-a"); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(dirA); !os.IsNotExist(err) {
		t.Fatalf("q-a dir should be removed after destroy: %v", err)
	}
	if _, err := os.Stat(dirB); err != nil {
		t.Fatalf("q-b dir should still exist after destroying q-a: %v", err)
	}
	if q := w.GetQueue("ns", "q-a"); q != nil {
		t.Fatal("queueWAL should be nil after DestroyQueue")
	}
	if err := w.DestroyQueue("ns", "q-a"); err != nil {
		t.Fatalf("destroying non-existent queue should return nil: %v", err)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// 15. VisitQueues: registra 3 queues, visita todas
// ═══════════════════════════════════════════════════════════════════════════════

func TestWAL_VisitQueues(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWAL(dir, walConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { safeClose(w) })

	queues := []struct{ ns, q string }{
		{"ns-a", "q-a"},
		{"ns-b", "q-b"},
		{"ns-c", "q-c"},
	}
	for _, q := range queues {
		e := testEvent(EventPublish, q.ns, q.q, "g", "m1", nil)
		_ = w.AppendSync(e)
	}
	w.Close()

	var visited []string
	visitMu := sync.Mutex{}
	w.VisitQueues(func(qw *QueueWAL) {
		visitMu.Lock()
		visited = append(visited, qw.Namespace+"/"+qw.Queue)
		visitMu.Unlock()
	})

	if len(visited) != len(queues) {
		t.Fatalf("visited %d queues, want %d", len(visited), len(queues))
	}
	m := make(map[string]bool)
	for _, v := range visited {
		m[v] = true
	}
	for _, q := range queues {
		key := q.ns + "/" + q.q
		if !m[key] {
			t.Errorf("queue %s was not visited", key)
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// 16. Healthy/Err: sem erro = Healthy, com erro = Err retorna
// ═══════════════════════════════════════════════════════════════════════════════

func TestWAL_Healthy(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWAL(dir, walConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { safeClose(w) })

	if !w.Healthy() {
		t.Error("expected Healthy()=true with no queues")
	}
	if err := w.Err(); err != nil {
		t.Errorf("expected Err()=nil, got %v", err)
	}

	e := testEvent(EventPublish, "ns", "q", "g", "m1", nil)
	_ = w.AppendSync(e)

	if !w.Healthy() {
		t.Error("expected Healthy()=true with only healthy queues")
	}
	if err := w.Err(); err != nil {
		t.Errorf("expected Err()=nil, got %v", err)
	}

	e = testEvent(EventPublish, "ns2", "q2", "g", "m1", nil)
	_ = w.AppendSync(e)
	qw := w.GetQueue("ns2", "q2")
	qw.SetErr(ErrWALFailed)

	if w.Healthy() {
		t.Error("expected Healthy()=false after setErr")
	}
	if err := w.Err(); err == nil {
		t.Fatal("expected non-nil error from Err()")
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// 17. Close: writer loop para limpo
// ═══════════════════════════════════════════════════════════════════════════════

func TestWAL_Close(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWAL(dir, walConfig())
	if err != nil {
		t.Fatal(err)
	}

	for i := range 10 {
		e := testEvent(EventPublish, "ns", "q", "g", fmt.Sprintf("m-%d", i), []byte("data"))
		_ = w.AppendSync(e)
	}

	qw := w.GetQueue("ns", "q")
	if qw == nil {
		t.Fatal("queueWAL is nil")
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// writer goroutine must have exited
	select {
	case <-qw.done:
	default:
		t.Fatal("writer goroutine did not exit after Close")
	}

	// all 10 events must be readable from disk
	events := readAllEvents(t, qw.dir)
	if len(events) != 10 {
		t.Fatalf("expected 10 events on disk after Close, got %d", len(events))
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// 18. Concorrência: múltiplas goroutines AppendSync + Append simultâneos
// ═══════════════════════════════════════════════════════════════════════════════

func TestWAL_Concurrency(t *testing.T) {
	dir := t.TempDir()
	cfg := walConfig()
	cfg.FlushMaxMessages = 10
	w, err := NewWAL(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	const n = 50
	errCh := make(chan error, 4*n)

	for i := range n {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			e := testEvent(EventPublish, "ns", "q",
				fmt.Sprintf("g%d", id%5),
				fmt.Sprintf("sync-%d", id),
				[]byte(fmt.Sprintf("payload-%d", id)))
			if err := w.AppendSync(e); err != nil {
				errCh <- fmt.Errorf("AppendSync(%d): %w", id, err)
			}
		}(i)
	}

	for i := range n {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			e := testEvent(EventLease, "ns", "q",
				fmt.Sprintf("g%d", id%5),
				fmt.Sprintf("lease-%d", id), nil)
			if err := w.Append(e); err != nil {
				errCh <- fmt.Errorf("Append(%d): %w", id, err)
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Error(err)
	}

	w.Close()

	qw := w.GetQueue("ns", "q")
	events := readAllEvents(t, qw.dir)
	if len(events) != 2*n {
		t.Fatalf("expected %d events, got %d", 2*n, len(events))
	}

	type groupState struct {
		lastSeq uint64
	}
	groups := make(map[string]*groupState)
	for _, ev := range events {
		gs, ok := groups[ev.Group]
		if !ok {
			groups[ev.Group] = &groupState{lastSeq: ev.Seq}
			continue
		}
		if ev.Seq <= gs.lastSeq {
			t.Errorf("seq regression in group %s: %d <= %d", ev.Group, ev.Seq, gs.lastSeq)
		}
		gs.lastSeq = ev.Seq
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// 19. (bonus) Channel full: channel capacity exceeded returns error
// ═══════════════════════════════════════════════════════════════════════════════

func TestWAL_ChannelFull(t *testing.T) {
	dir := t.TempDir()
	cfg := walConfig()
	cfg.FlushInterval = time.Hour
	cfg.FlushMaxMessages = 100000
	w, err := NewWAL(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { safeClose(w) })

	qw := w.getQueueWAL("ns", "q")

	// Stop the original writer loop FIRST — it reads qw.ch on every
	// select iteration, so we must not touch qw.ch while it is running.
	origCh := qw.ch
	close(qw.stop)
	<-qw.done

	// Now safe to swap the channel (no goroutine is reading qw.ch).
	tinyCh := make(chan Event, 2)
	qw.ch = tinyCh
	qw.stop = make(chan struct{})
	qw.done = make(chan struct{})
	go qw.writerLoop()

	e := testEvent(EventPublish, "ns", "q", "g", "m1", nil)
	if err := w.Append(e); err != nil {
		t.Fatalf("expected first Append to succeed: %v", err)
	}
	if err := w.Append(e); err != nil {
		t.Fatalf("expected second Append to succeed: %v", err)
	}
	if err := w.Append(e); err == nil {
		t.Fatal("expected 'channel full' error")
	}

	// Stop tiny-channel loop before restoring origCh.
	close(qw.stop)
	<-qw.done

	// Restore so t.Cleanup (safeClose) can drain cleanly.
	qw.ch = origCh
	qw.stop = make(chan struct{})
	qw.done = make(chan struct{})
	go qw.writerLoop()
}

// ═══════════════════════════════════════════════════════════════════════════════
// 20. Per-queue durable override
// ═══════════════════════════════════════════════════════════════════════════════

// TestWAL_DefaultDurable verifica que DefaultDurable retorna o valor global.
func TestWAL_DefaultDurable(t *testing.T) {
	dir := t.TempDir()

	cfg := walConfig()
	cfg.Durable = true
	w, _ := NewWAL(dir, cfg)
	defer w.Close()

	if !w.DefaultDurable() {
		t.Fatal("DefaultDurable deve retornar true quando cfg.Durable=true")
	}

	cfg2 := walConfig()
	cfg2.Durable = false
	w2, _ := NewWAL(t.TempDir(), cfg2)
	defer w2.Close()

	if w2.DefaultDurable() {
		t.Fatal("DefaultDurable deve retornar false quando cfg.Durable=false")
	}
}

// TestWAL_QueueDurable_FallsBackToGlobal verifica que filas sem override herdam o global.
func TestWAL_QueueDurable_FallsBackToGlobal(t *testing.T) {
	dir := t.TempDir()
	cfg := walConfig()
	cfg.Durable = true
	w, _ := NewWAL(dir, cfg)
	defer w.Close()

	// Sem override — deve retornar o global
	if !w.QueueDurable("ns", "myqueue") {
		t.Fatal("sem override deve retornar global (true)")
	}
}

// TestWAL_SetQueueDurable_OverridesGlobal verifica o override por fila antes de criar o WAL.
func TestWAL_SetQueueDurable_OverridesGlobal(t *testing.T) {
	dir := t.TempDir()
	cfg := walConfig()
	cfg.Durable = true // global = durable
	w, _ := NewWAL(dir, cfg)
	defer w.Close()

	// Override: esta fila não é durable
	w.SetQueueDurable("ns", "fast-queue", false)

	if w.QueueDurable("ns", "fast-queue") {
		t.Fatal("fila com durable=false não deve retornar true")
	}
	// Fila sem override mantém o global
	if !w.QueueDurable("ns", "other-queue") {
		t.Fatal("fila sem override deve herdar global (true)")
	}
}

// TestWAL_SetQueueDurable_AfterFirstMessage verifica que o override aplicado APÓS
// a primeira mensagem atualiza o cfg do QueueWAL já existente.
func TestWAL_SetQueueDurable_AfterFirstMessage(t *testing.T) {
	dir := t.TempDir()
	cfg := walConfig()
	cfg.Durable = true
	w, _ := NewWAL(dir, cfg)
	defer w.Close()

	// Primeira mensagem — cria o QueueWAL com Durable=true
	e := testEvent(EventPublish, "ns", "late-queue", "g", "m1", []byte("x"))
	if err := w.AppendSync(e); err != nil {
		t.Fatalf("AppendSync: %v", err)
	}

	qw := w.GetQueue("ns", "late-queue")
	if qw == nil {
		t.Fatal("QueueWAL não criado")
	}
	if !qw.cfg.Durable {
		t.Fatal("queueWAL deve iniciar com Durable=true (global)")
	}

	// Override aplicado DEPOIS da criação
	w.SetQueueDurable("ns", "late-queue", false)

	qw.mu.Lock()
	durableAfter := qw.cfg.Durable
	qw.mu.Unlock()

	if durableAfter {
		t.Fatal("SetQueueDurable deve atualizar o QueueWAL existente")
	}
	if w.QueueDurable("ns", "late-queue") {
		t.Fatal("QueueDurable deve refletir o override")
	}
}

// TestWAL_NonDurableQueue_WritesEvents verifica que uma fila não-durable
// ainda grava eventos corretamente (só sem fsync).
func TestWAL_NonDurableQueue_WritesEvents(t *testing.T) {
	dir := t.TempDir()
	cfg := walConfig()
	cfg.Durable = false
	w, _ := NewWAL(dir, cfg)

	const n = 20
	for i := range n {
		e := testEvent(EventPublish, "ns", "q", "g", fmt.Sprintf("msg-%d", i), []byte("payload"))
		if err := w.AppendSync(e); err != nil {
			t.Fatalf("AppendSync(%d): %v", i, err)
		}
	}

	qw := w.GetQueue("ns", "q")
	w.Close() // fecha uma única vez; não usar defer aqui

	events := readAllEvents(t, qw.dir)
	if len(events) != n {
		t.Fatalf("esperava %d eventos, got %d", n, len(events))
	}
}
