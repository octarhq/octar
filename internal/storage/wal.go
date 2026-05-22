// Package storage implements OCTAR's Write-Ahead Log (WAL).
//
// Architecture: channel-based single-writer per queue.
//
//	producers → append channel → writer goroutine → batch encode → disk
//
// File layout:
//
//	event.go    — EventType constants + Event struct
//	wal.go      — WAL + queueWAL structs, lifecycle (New / Append / Close)
//	segment.go  — segment open/rotate, newQueueWAL, findLastSegmentID
//	writer.go   — writeRecord, flushBatch, writerLoop (encode + I/O)
//	snapshots.go — SaveSnapshot, state-to-snapshot conversion
//	reader.go   — ReadEvent (WAL record decode + CRC), LoadSnapshot
package storage

import (
	"errors"
	"fmt"
	"hash"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// appendSyncTimeout is the maximum time AppendSync waits for the WAL batch
// containing its event to be flushed and fsynced. If the WAL writer is stuck
// (disk hang, queue full), the caller gets an error instead of blocking forever.
const appendSyncTimeout = 30 * time.Second

// WALConfig controls flush behaviour, segment rotation, and snapshot cadence.
type WALConfig struct {
	FlushInterval      time.Duration
	FlushMaxMessages   int
	SegmentMaxBytes    int64
	Sync               bool
	SnapshotInterval   time.Duration
	QueueStateProvider func(namespace, queue string) interface{}
}

// WAL is the broker-level WAL that owns one queueWAL per (namespace, queue).
type WAL struct {
	rootDir string
	cfg     WALConfig
	queues  map[string]*QueueWAL
	mu      sync.RWMutex
	logger  *slog.Logger
}

// QueueWAL is a single-writer WAL for one (namespace, queue) pair.
// All fields other than ch/stop/done are accessed only by the writer goroutine
// (under q.mu where noted).
type QueueWAL struct {
	dir string
	cfg WALConfig
	ch  chan Event
	stop chan struct{}
	done chan struct{}

	mu        sync.Mutex
	seq       uint64
	segmentID uint64
	file      *os.File
	logBuf    interface {
		Flush() error
		Write([]byte) (int, error)
		WriteString(string) (int, error)
	}
	idxFile *os.File
	idxBuf  interface {
		Flush() error
		Write([]byte) (int, error)
	}
	fileSize int64
	logger   *slog.Logger

	Namespace     string
	Queue         string
	stateProvider func() interface{}
	crcHash       hash.Hash32
	lastSnapSegID atomic.Uint64 // segment ID of the most recent confirmed snapshot
	snapshotSem   chan struct{} // capacity 1: prevents concurrent snapshot goroutines
	errored       atomic.Value // stores error when WAL enters permanent failure; nil = healthy
}

// newQueueLogger is a shared helper so segment.go and other files use the same
// logger attributes without importing slog directly.
func newQueueLogger(namespace, queue string) *slog.Logger {
	return slog.Default().With("component", "wal", "namespace", namespace, "queue", queue)
}

// NewWAL creates the root WAL directory and returns an empty WAL.
// Queue-specific writers are created lazily on first Append.
func NewWAL(rootDir string, cfg WALConfig) (*WAL, error) {
	if err := os.MkdirAll(rootDir, 0755); err != nil {
		return nil, fmt.Errorf("wal: create root dir: %w", err)
	}
	return &WAL{
		rootDir: rootDir,
		cfg:     cfg,
		queues:  make(map[string]*QueueWAL),
		logger:  slog.Default().With("component", "wal"),
	}, nil
}

// Append sends event e to the per-queue writer goroutine without blocking.
// Use this for LEASE, ACK, NACK, and EXPIRE — events where the broker can tolerate
// a tiny window of potential loss between the channel send and the next fsync.
//
// Non-blocking: if the WAL channel (capacity 8192) is full the event is dropped
// with an error. The caller must handle the error gracefully — for at-least-once
// delivery the in-memory state is the truth and the WAL gap is recovered on restart.
//
// Returns ErrWALFailed if the WAL writer has permanently failed (disk full, panic).
func (w *WAL) Append(e Event) error {
	qw := w.getQueueWAL(e.Namespace, e.Queue)
	if err := qw.Err(); err != nil {
		return err
	}
	select {
	case qw.ch <- e:
		return nil
	default:
		return fmt.Errorf("wal: channel full for %s/%s", e.Namespace, e.Queue)
	}
}

// Err returns nil if the queue WAL is healthy, or a permanent failure reason
// (e.g. ENOSPC, panic) if the writer has irrecoverably failed.
func (q *QueueWAL) Err() error {
	if v := q.errored.Load(); v != nil {
		return v.(error)
	}
	return nil
}

// Healthy returns true if every queue WAL is healthy (no permanent errors).
func (w *WAL) Healthy() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	for _, q := range w.queues {
		if q.Err() != nil {
			return false
		}
	}
	return true
}

// Err returns the first permanent error across all queue WALs, or nil.
func (w *WAL) Err() error {
	w.mu.RLock()
	defer w.mu.RUnlock()
	for _, q := range w.queues {
		if err := q.Err(); err != nil {
			return err
		}
	}
	return nil
}

// SetErr marks this queueWAL as permanently failed. All subsequent
// Append/AppendSync calls will return this error. After setting the error the
// writer goroutine should stop processing (the channel contents are discarded).
func (q *QueueWAL) SetErr(err error) {
	q.errored.Store(err)
	q.logger.Error("wal permanently failed", "error", err)
}

// ErrWALFailed is returned by Append/AppendSync when the WAL writer has
// entered an irrecoverable state (disk full, writer panic). The broker should
// refuse new publishes until the operator intervenes.
var ErrWALFailed = errors.New("wal: permanently failed")

// AppendSync sends event e and blocks until the batch containing it has been
// written and fsynced (when cfg.Sync=true) or at least buffer-flushed.
//
// Use this for PUBLISH: the durability contract states that if the broker sent
// ACK to the publisher, the message must survive a crash. AppendSync enforces
// that contract by making the caller wait for disk confirmation before returning.
//
// The batch window (FlushInterval / FlushMaxMessages) still applies — multiple
// concurrent publishers share the same fsync, so throughput scales with concurrency
// even though each caller individually waits.
//
// Timeout: if no response arrives within appendSyncTimeout (30s) the caller
// unblocks with an error. This prevents goroutine leaks when the WAL channel is
// full or the disk is hung.
func (w *WAL) AppendSync(e Event) error {
	qw := w.getQueueWAL(e.Namespace, e.Queue)
	if err := qw.Err(); err != nil {
		return err
	}
	e.done = make(chan error, 1) // buffered: writer never blocks even if caller times out
	select {
	case qw.ch <- e:
		// event queued — wait for flush confirmation or timeout
	case <-time.After(appendSyncTimeout):
		return fmt.Errorf("wal: append sync timed out after %v (channel full)", appendSyncTimeout)
	}
	t := time.NewTimer(appendSyncTimeout)
	defer t.Stop()
	select {
	case err := <-e.done:
		return err
	case <-t.C:
		return fmt.Errorf("wal: append sync timed out after %v (no flush confirmation)", appendSyncTimeout)
	}
}

// RegisterQueueState registers a state provider callback so the WAL can
// include queue state in snapshots.
func (w *WAL) RegisterQueueState(namespace, queue string, stateProvider func() interface{}) {
	key := namespace + "/" + queue
	w.mu.Lock()
	defer w.mu.Unlock()
	if q, ok := w.queues[key]; ok {
		q.stateProvider = stateProvider
	}
}

// Close stops all per-queue writer goroutines and flushes pending data.
func (w *WAL) Close() error {
	w.mu.Lock() // need write lock to iterate + remove queueWALs
	defer w.mu.Unlock()

	for _, q := range w.queues {
		w.closeQueueWAL(q)
	}
	return nil
}

// DestroyQueue stops the writer, closes files, and removes all .log/.idx/.snap
// files for the given (namespace, queue). Returns nil if the queue never existed.
func (w *WAL) DestroyQueue(namespace, queue string) error {
	key := namespace + "/" + queue
	w.mu.Lock()
	q, ok := w.queues[key]
	if !ok {
		w.mu.Unlock()
		return nil
	}
	delete(w.queues, key)
	w.mu.Unlock()

	w.closeQueueWAL(q)
	return os.RemoveAll(q.dir)
}

func (w *WAL) closeQueueWAL(q *QueueWAL) {
	close(q.stop)
	<-q.done

	q.mu.Lock()
	if q.file != nil {
		q.file.Close()
	}
	if q.idxBuf != nil {
		q.idxBuf.Flush()
	}
	if q.idxFile != nil {
		q.idxFile.Close()
	}
	q.mu.Unlock()
}

// GetQueue returns the queueWAL for the given (namespace, queue), or nil if
// the queue has no WAL yet (no messages published).
func (w *WAL) GetQueue(namespace, queue string) *QueueWAL {
	key := namespace + "/" + queue
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.queues[key]
}

// VisitQueues calls fn for each queueWAL. The function is called under a read
// lock; it must not block or call back into the WAL.
func (w *WAL) VisitQueues(fn func(qw *QueueWAL)) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	for _, qw := range w.queues {
		fn(qw)
	}
}

// getQueueWAL returns (or lazily creates) the queueWAL for the given key.
func (w *WAL) getQueueWAL(namespace, queue string) *QueueWAL {
	key := namespace + "/" + queue

	w.mu.RLock()
	q, ok := w.queues[key]
	w.mu.RUnlock()
	if ok {
		return q
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if existing, ok := w.queues[key]; ok {
		return existing
	}
	newQ := newQueueWAL(w.rootDir, namespace, queue, w.cfg)
	w.queues[key] = newQ
	return newQ
}
