package chaos

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestCorruption_LogRecord corrupts a single byte in the log segment to trigger
// a CRC mismatch. Recovery must skip the corrupted record and continue.
func TestCorruption_LogRecord(t *testing.T) {
	h := New(t)
	defer h.Close()

	h.RegisterQueue("test-ns", "corrupt-log")

	for i := 0; i < 20; i++ {
		_, err := h.Publish("test-ns", "corrupt-log", "g1", []byte("data"))
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	time.Sleep(200 * time.Millisecond)

	// Find the log segment
	qDir := filepath.Join(h.WalDir, "test-ns", "corrupt-log")
	var logPath string
	entries, _ := os.ReadDir(qDir)
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".log" {
			logPath = filepath.Join(qDir, e.Name())
		}
	}
	if logPath == "" {
		t.Fatal("no log file found")
	}

	h.Wal.Close()

	// Corrupt a byte near the end of the segment to leave most records intact
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) > 10 {
		corruptOffset := len(data) - 10
		data[corruptOffset] ^= 0xFF
		if err := os.WriteFile(logPath, data, 0644); err != nil {
			t.Fatal(err)
		}
	}

	h.Restart()

	q2 := h.GetQueue("test-ns", "corrupt-log")
	if q2 == nil {
		t.Fatal("queue not recovered")
	}
	stats, _ := q2.Snapshot()
	var total int
	for _, g := range stats.Groups {
		total += g.Pending + g.Processing
	}
	if total < 18 {
		t.Fatalf("expected at least 18 good records recovered, got %d", total)
	}
}

// TestCorruption_Snapshot corrupts the latest snapshot to test N-1 fallback.
func TestCorruption_Snapshot(t *testing.T) {
	h := New(t)
	defer h.Close()

	h.RegisterQueue("test-ns", "corrupt-snap")

	for i := 0; i < 30; i++ {
		_, err := h.Publish("test-ns", "corrupt-snap", "g1", []byte("data"))
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	// Force a snapshot
	h.Wal.GetQueue("test-ns", "corrupt-snap").SaveSnapshot()

	qDir := filepath.Join(h.WalDir, "test-ns", "corrupt-snap")

	// Find the latest snapshot
	var snapPath string
	var maxSeq uint64
	entries, _ := os.ReadDir(qDir)
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".snap" {
			var seq uint64
			if _, err := filepath.Base(e.Name()), &seq; err == nil && seq > maxSeq {
				_ = filepath.Base(e.Name())
			}
		}
	}
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".snap" {
			snapPath = filepath.Join(qDir, e.Name())
		}
	}
	if snapPath == "" {
		t.Fatal("no snapshot found")
	}

	h.Wal.Close()

	// Corrupt the snapshot by writing garbage over the magic bytes
	data, err := os.ReadFile(snapPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) > 4 {
		copy(data[0:4], []byte("BADC"))
		if err := os.WriteFile(snapPath, data, 0644); err != nil {
			t.Fatal(err)
		}
	}

	h.Restart()

	q2 := h.GetQueue("test-ns", "corrupt-snap")
	if q2 == nil {
		t.Fatal("queue not recovered")
	}
	stats, _ := q2.Snapshot()
	var total int
	for _, g := range stats.Groups {
		total += g.Pending + g.Processing
	}
	// Should still recover messages (either from N-1 snapshot or full replay)
	if total != 30 {
		t.Fatalf("expected 30 messages after corrupted snapshot recovery, got %d", total)
	}
}

// TestCorruption_IndexFile corrupts the index file; recovery should ignore
// index errors and replay from the log segments directly.
func TestCorruption_IndexFile(t *testing.T) {
	h := New(t)
	defer h.Close()

	h.RegisterQueue("test-ns", "corrupt-idx")

	for i := 0; i < 10; i++ {
		_, err := h.Publish("test-ns", "corrupt-idx", "g1", []byte("data"))
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	time.Sleep(200 * time.Millisecond)

	qDir := filepath.Join(h.WalDir, "test-ns", "corrupt-idx")
	var idxPath string
	entries, _ := os.ReadDir(qDir)
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".idx" {
			idxPath = filepath.Join(qDir, e.Name())
		}
	}
	if idxPath == "" {
		t.Fatal("no index file found")
	}

	h.Wal.Close()

	// Wipe the index file
	if err := os.Truncate(idxPath, 0); err != nil {
		t.Fatal(err)
	}

	h.Restart()

	q2 := h.GetQueue("test-ns", "corrupt-idx")
	if q2 == nil {
		t.Fatal("queue not recovered")
	}
	stats, _ := q2.Snapshot()
	var total int
	for _, g := range stats.Groups {
		total += g.Pending + g.Processing
	}
	if total != 10 {
		t.Fatalf("expected 10 messages after corrupted index recovery, got %d", total)
	}
}
