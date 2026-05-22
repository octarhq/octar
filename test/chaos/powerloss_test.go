package chaos

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestPowerLoss_PartialSegment truncates the last log segment to simulate a
// power loss mid-write. Recovery must skip the partial (CRC-invalid) record.
func TestPowerLoss_PartialSegment(t *testing.T) {
	h := New(t)
	defer h.Close()

	h.RegisterQueue("test-ns", "powerloss-partial")

	for i := 0; i < 10; i++ {
		_, err := h.Publish("test-ns", "powerloss-partial", "g1", []byte("hello"))
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	// Let WAL flush to disk
	time.Sleep(200 * time.Millisecond)

	// Find the last log segment
	qDir := filepath.Join(h.WalDir, "test-ns", "powerloss-partial")
	entries, err := os.ReadDir(qDir)
	if err != nil {
		t.Fatal(err)
	}

	var lastLog string
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".log" {
			lastLog = filepath.Join(qDir, e.Name())
		}
	}
	if lastLog == "" {
		t.Fatal("no log segment found")
	}

	// Read current size
	info, err := os.Stat(lastLog)
	if err != nil {
		t.Fatal(err)
	}
	origSize := info.Size()

	// Hard crash: close WAL without flush
	h.Wal.Close()

	// Truncate the last 100 bytes to simulate partial write
	if err := os.Truncate(lastLog, origSize-100); err != nil {
		t.Fatal(err)
	}

	// Restart and verify the 10 recoverable messages
	h.Restart()

	q2 := h.GetQueue("test-ns", "powerloss-partial")
	if q2 == nil {
		t.Fatal("queue not recovered")
	}
	stats, _ := q2.Snapshot()
	var total int
	for _, g := range stats.Groups {
		total += g.Pending + g.Processing
	}
	if total < 8 {
		t.Fatalf("expected at least 8 messages after partial-write recovery, got %d", total)
	}
	if total > 10 {
		t.Fatalf("expected at most 10 messages, got %d", total)
	}
}

// TestPowerLoss_EmptySegment simulates a power loss that created an empty
// segment file. Recovery must handle empty files gracefully.
func TestPowerLoss_EmptySegment(t *testing.T) {
	h := New(t)
	defer h.Close()

	h.RegisterQueue("test-ns", "powerloss-empty")

	for i := 0; i < 5; i++ {
		_, err := h.Publish("test-ns", "powerloss-empty", "g1", []byte("data"))
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	h.Wal.Close()

	// Create an empty segment with a higher ID
	qDir := filepath.Join(h.WalDir, "test-ns", "powerloss-empty")
	emptyFile := filepath.Join(qDir, "000099999.log")
	f, err := os.Create(emptyFile)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	h.Restart()

	q2 := h.GetQueue("test-ns", "powerloss-empty")
	if q2 == nil {
		t.Fatal("queue not recovered")
	}
	stats, _ := q2.Snapshot()
	var total int
	for _, g := range stats.Groups {
		total += g.Pending + g.Processing
	}
	if total != 5 {
		t.Fatalf("expected 5 messages, got %d", total)
	}
}

// TestPowerLoss_NoFlushBeforeCrash ensures messages already sent to the WAL
// channel but not yet flushed survive a crash-then-restart.
func TestPowerLoss_NoFlushBeforeCrash(t *testing.T) {
	h := New(t)
	defer h.Close()

	h.RegisterQueue("test-ns", "powerloss-noflush")

	for i := 0; i < 10; i++ {
		_, err := h.Publish("test-ns", "powerloss-noflush", "g1", []byte("survive"))
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	// Immediate crash without any flush delay
	h.Crash()
	h.Restart()

	q2 := h.GetQueue("test-ns", "powerloss-noflush")
	if q2 == nil {
		t.Fatal("queue not recovered")
	}
	stats, _ := q2.Snapshot()
	var total int
	for _, g := range stats.Groups {
		total += g.Pending + g.Processing
	}
	if total != 10 {
		t.Fatalf("expected 10 messages, got %d", total)
	}
}
