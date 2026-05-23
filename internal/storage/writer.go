package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"time"
	"unsafe"

	"github.com/octarhq/octar/internal/metrics"
	"github.com/octarhq/octar/internal/queue"
	"github.com/octarhq/octar/internal/storage/snapshot"
)

// isDiskFull returns true when err is an ENOSPC (no space left on device) or
// EDQUOT (disk quota exceeded) error. This is platform-dependent; we match on
// the error message which is portable across Go's supported OS targets.
func isDiskFull(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return errors.Is(err, os.ErrClosed) ||
		containsAny(msg, "no space left on device", "disk quota exceeded", "not enough space")
}

func containsAny(s string, substrs ...string) bool {
	for _, sub := range substrs {
		if len(sub) <= len(s) {
			for i := 0; i <= len(s)-len(sub); i++ {
				if s[i:i+len(sub)] == sub {
					return true
				}
			}
		}
	}
	return false
}

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// stringBytes converts a string to a []byte without allocation.
// The caller must not modify the returned slice.
func stringBytes(s string) []byte {
	if len(s) == 0 {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

// ── WAL record encoding ───────────────────────────────────────────────────────

// writeRecord encodes event e into the buffered log and index writers.
// Returns the number of bytes written (used to update fileSize).
// Must be called with q.mu held.
//
// CRC is computed via streaming hash to avoid allocating a crcData slice that
// duplicates the entire record in memory. For large payloads (multi-MB) this
// is the difference between O(1) and O(N) memory per event.
func (q *QueueWAL) writeRecord(e Event) int {
	posBefore := q.fileSize

	payloadLen := len(e.Payload)
	fixedOverhead := 21 + 2 + len(e.Namespace) + 2 + len(e.Queue) +
		2 + len(e.Group) + 2 + len(e.MsgID) + 4 + payloadLen + 4

	// Header: seq (8) | type (1) | timestamp (8)
	var header [21]byte
	binary.BigEndian.PutUint64(header[0:], e.Seq)
	header[8] = byte(e.Type)
	binary.BigEndian.PutUint64(header[9:], uint64(e.Timestamp))

	var payloadLenBuf [4]byte
	binary.BigEndian.PutUint32(payloadLenBuf[:], uint32(payloadLen))

	// Streaming CRC — zero allocation for the data copy.
	h := q.crcHash
	h.Reset()
	h.Write(header[:])

	var fieldLenBuf [2]byte
	for _, f := range []string{e.Namespace, e.Queue, e.Group, e.MsgID} {
		binary.BigEndian.PutUint16(fieldLenBuf[:], uint16(len(f)))
		h.Write(fieldLenBuf[:])
		h.Write(stringBytes(f))
	}
	h.Write(payloadLenBuf[:])
	if payloadLen > 0 {
		h.Write(e.Payload)
	}

	var footer [4]byte
	binary.BigEndian.PutUint32(footer[:], h.Sum32())

	// Write to buffered log
	_, _ = q.logBuf.Write(header[:])
	for _, f := range []string{e.Namespace, e.Queue, e.Group, e.MsgID} {
		binary.BigEndian.PutUint16(fieldLenBuf[:], uint16(len(f)))
		_, _ = q.logBuf.Write(fieldLenBuf[:])
		_, _ = q.logBuf.WriteString(f)
	}
	_, _ = q.logBuf.Write(payloadLenBuf[:])
	if payloadLen > 0 {
		_, _ = q.logBuf.Write(e.Payload)
	}
	_, _ = q.logBuf.Write(footer[:])

	// Sparse index entry: seq (8) | file position (8)
	var idxEntry [16]byte
	binary.BigEndian.PutUint64(idxEntry[0:], e.Seq)
	binary.BigEndian.PutUint64(idxEntry[8:], uint64(posBefore))
	_, _ = q.idxBuf.Write(idxEntry[:])

	return fixedOverhead
}

// ── Batch flush ───────────────────────────────────────────────────────────────

// flushBatch writes a batch of events to disk atomically (one fsync covers the
// whole batch). Triggers a snapshot + segment rotation when the size limit is hit.
//
// Sync waiters: events with a non-nil done channel are waiting for this flush
// to complete (AppendSync callers). We signal them — with the flush error if
// anything went wrong — before returning, so they are never left blocking.
func (q *QueueWAL) flushBatch(batch []Event) {
	if len(batch) == 0 {
		return
	}
	start := time.Now()

	q.mu.Lock()
	defer q.mu.Unlock()

	// signalDone notifies every sync waiter in the batch with the given error.
	// Called unconditionally on all exit paths so AppendSync never deadlocks.
	signalDone := func(err error) {
		for _, e := range batch {
			if e.done != nil {
				e.done <- err
			}
		}
	}

	firstSeq := q.seq + 1
	for i := range batch {
		q.seq++
		batch[i].Seq = q.seq
		batch[i].Timestamp = time.Now().UnixNano()
		q.fileSize += int64(q.writeRecord(batch[i]))
	}

	flushStart := time.Now()
	if err := q.logBuf.Flush(); err != nil {
		q.logger.Error("wal log flush failed", "error", err)
		if isDiskFull(err) {
			q.logger.Error("wal: disk full, enters permanent failure", "error", err)
			metrics.WALDiskFull.WithLabelValues(q.Namespace, q.Queue).Inc()
			q.SetErr(fmt.Errorf("%w: %s", ErrWALFailed, err))
		}
		signalDone(err)
		return
	}

	var fsyncDur time.Duration
	if q.cfg.Durable {
		t := time.Now()
		if err := q.file.Sync(); err != nil {
			q.logger.Error("wal fsync failed", "error", err)
			if isDiskFull(err) {
				q.logger.Error("wal: disk full, enters permanent failure", "error", err)
				metrics.WALDiskFull.WithLabelValues(q.Namespace, q.Queue).Inc()
				q.SetErr(fmt.Errorf("%w: %s", ErrWALFailed, err))
			}
			signalDone(err)
			return
		}
		fsyncDur = time.Since(t)
		metrics.WALFSyncDuration.WithLabelValues(q.Namespace, q.Queue).Observe(fsyncDur.Seconds())
	}

	// Disk write confirmed — unblock all AppendSync callers in this batch.
	signalDone(nil)

	if err := q.idxBuf.Flush(); err != nil {
		q.logger.Error("wal idx flush failed", "error", err)
	}

	rotated := false
	if q.fileSize >= q.cfg.SegmentMaxBytes {
		go q.saveSnapshotRecovery() // async: I/O without holding the lock
		_ = q.rotateSegment()
		rotated = true
		metrics.WALSegmentRotations.WithLabelValues(q.Namespace, q.Queue).Inc()
	}

	metrics.WALFlushDuration.WithLabelValues(q.Namespace, q.Queue).Observe(time.Since(start).Seconds())
	metrics.WALBatchSize.WithLabelValues(q.Namespace, q.Queue).Observe(float64(len(batch)))
	metrics.WALQueueDepth.WithLabelValues(q.Namespace, q.Queue).Set(float64(len(q.ch)))

	q.logger.Debug("wal flushed",
		"events", len(batch),
		"bytes", q.fileSize,
		"segment", q.segmentID,
		"first_seq", firstSeq,
		"flush_ms", time.Since(flushStart).Milliseconds(),
		"fsync_ms", fsyncDur.Milliseconds(),
		"rotated", rotated,
	)
}

// ── Writer goroutine ──────────────────────────────────────────────────────────

// writerLoop is the single goroutine that owns all writes for this queue.
// It collects events into batches and flushes on timer or size threshold.
func (q *QueueWAL) writerLoop() {
	defer func() {
		if r := recover(); r != nil {
			q.logger.Error("wal writer panicked", "panic", r)
			q.SetErr(fmt.Errorf("wal writer panic: %v", r))
			// Close the done channel so blocked AppendSync callers unblock.
			// Don't re-close if writerLoop already closed it on clean stop.
			select {
			case <-q.done:
			default:
				close(q.done)
			}
		}
	}()

	var batch []Event

	var flushTimer *time.Ticker
	if q.cfg.FlushInterval > 0 {
		flushTimer = time.NewTicker(q.cfg.FlushInterval)
		defer flushTimer.Stop()
	}

	var snapshotTimer *time.Ticker
	if q.cfg.SnapshotInterval > 0 {
		snapshotTimer = time.NewTicker(q.cfg.SnapshotInterval)
		defer snapshotTimer.Stop()
	}

	flush := func() {
		if len(batch) > 0 {
			q.flushBatch(batch)
			batch = batch[:0]
		}
	}

	for {
		if q.Err() != nil {
			// WAL permanently failed — drain remaining events to unblock
			// callers, then stop.
		errDrain:
			for {
				select {
				case e, ok := <-q.ch:
					if !ok {
						break errDrain
					}
					if e.done != nil {
						e.done <- q.Err()
					}
				default:
					break errDrain
				}
			}
			close(q.done)
			return
		}

		// Assign channels defensively — reading from a nil channel blocks forever,
		// which is the correct behaviour when the timer is disabled.
		var flushC, snapC <-chan time.Time
		if flushTimer != nil {
			flushC = flushTimer.C
		}
		if snapshotTimer != nil {
			snapC = snapshotTimer.C
		}

		select {
		case e, ok := <-q.ch:
			if !ok {
				flush()
				q.logger.Info("channel closed, writer exiting")
				close(q.done)
				return
			}
			batch = append(batch, e)
			if len(batch) >= q.cfg.FlushMaxMessages {
				flush()
			}
		case <-flushC:
			flush()
		case <-snapC:
			go q.saveSnapshotRecovery()
		case <-q.stop:
			// Drain remaining events from the channel before flushing,
			// so AppendSync callers don't block forever during shutdown.
		drainLoop:
			for {
				select {
				case e, ok := <-q.ch:
					if !ok {
						break drainLoop
					}
					batch = append(batch, e)
					if len(batch) >= q.cfg.FlushMaxMessages {
						flush()
					}
				default:
					break drainLoop
				}
			}
			flush()
			q.logger.Info("writer stopped")
			close(q.done)
			return
		}
	}
}

// ── Snapshot saving ───────────────────────────────────────────────────────────

// SaveSnapshot is the public entry point used by tests and explicit callers.
func (q *QueueWAL) SaveSnapshot() error {
	return q.saveSnapshot()
}

// saveSnapshot captures the current queue state and writes a .snap file.
// Designed to run in a goroutine: it holds q.mu only for the metadata copy,
// then does all I/O outside the lock.
//
// Concurrency: the snapshotSem channel (capacity 1) ensures only one snapshot
// goroutine runs at a time. If a snapshot is already in progress, this call
// returns immediately. This prevents goroutine pileup under heavy segment
// rotation pressure.
func (q *QueueWAL) saveSnapshot() error {
	select {
	case q.snapshotSem <- struct{}{}:
		defer func() { <-q.snapshotSem }()
	default:
		q.logger.Warn("snapshot already in progress, skipping")
		return nil
	}

	start := time.Now()
	defer func() {
		metrics.SnapshotDuration.
			WithLabelValues(q.Namespace, q.Queue).
			Observe(time.Since(start).Seconds())
	}()

	q.mu.Lock()
	seq := q.seq
	segID := q.segmentID
	q.mu.Unlock()

	var stateVal interface{}
	if q.stateProvider != nil {
		stateVal = q.stateProvider()
	}

	snapPath := filepath.Join(q.dir, fmt.Sprintf("%09d.snap", segID))
	f, err := os.Create(snapPath)
	if err != nil {
		metrics.SnapshotFailures.WithLabelValues(q.Namespace, q.Queue).Inc()
		return fmt.Errorf("snapshot: create: %w", err)
	}
	defer f.Close()

	snap := &snapshot.Snapshot{
		SegmentID: segID,
		WALSeq:    seq,
		Timestamp: time.Now().UnixNano(),
	}
	if stateVal != nil {
		if state, ok := stateVal.(queue.QueueState); ok {
			snap.Groups = convertQueueStateToSnapshot(state)
		}
	}

	if err := snapshot.NewWriter(f).Write(snap); err != nil {
		metrics.SnapshotFailures.WithLabelValues(q.Namespace, q.Queue).Inc()
		return fmt.Errorf("snapshot: write: %w", err)
	}
	if err := f.Sync(); err != nil {
		metrics.SnapshotFailures.WithLabelValues(q.Namespace, q.Queue).Inc()
		return fmt.Errorf("snapshot: sync: %w", err)
	}

	q.lastSnapSegID.Store(segID)
	q.removeOldSegments(segID)

	q.logger.Info("snapshot saved",
		"path", snapPath,
		"segment", segID,
		"wal_seq", seq,
		"groups", len(snap.Groups),
		"cleaned_before", segID,
	)
	return nil
}

// saveSnapshotRecovery wraps saveSnapshot with panic recovery for goroutine safety.
func (q *QueueWAL) saveSnapshotRecovery() {
	defer func() {
		if r := recover(); r != nil {
			q.logger.Error("snapshot panicked", "panic", r)
		}
	}()
	if err := q.saveSnapshot(); err != nil {
		q.logger.Error("snapshot failed", "error", err)
	}
}

// convertQueueStateToSnapshot translates live queue state into the snapshot
// wire format, capturing ready, delayed, and processing messages.
func convertQueueStateToSnapshot(state queue.QueueState) []snapshot.GroupSnapshot {
	result := make([]snapshot.GroupSnapshot, 0, len(state.Groups))

	for key, gs := range state.Groups {
		sg := snapshot.GroupSnapshot{
			Key:         key,
			Parallelism: int32(gs.Parallelism),
			Quantum:     int32(gs.Quantum),
		}

		all := make([]*queue.Message, 0,
			len(gs.ReadyMsgs)+len(gs.UrgentMsgs)+len(gs.DelayedMsgs)+len(gs.ProcessingMsgs))
		all = append(all, gs.ReadyMsgs...)
		all = append(all, gs.UrgentMsgs...)
		all = append(all, gs.DelayedMsgs...)
		for _, m := range gs.ProcessingMsgs {
			all = append(all, m)
		}

		sg.Messages = make([]snapshot.MessageSnapshot, len(all))
		for i, m := range all {
			sg.Messages[i] = snapshot.MessageSnapshot{
				ID:          m.ID,
				Payload:     m.Payload,
				State:       int8(m.State),
				Attempts:    int32(m.Attempts),
				CreatedAt:   m.CreatedAt.UnixNano(),
				ScheduledAt: m.ScheduledAt.UnixNano(),
				LastError:   m.LastError,
			}
		}
		result = append(result, sg)
	}
	return result
}
