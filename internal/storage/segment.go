package storage

import (
	"bufio"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
)

// newQueueWAL creates the per-queue WAL writer, opens the latest segment, and
// launches the background writer goroutine.
func newQueueWAL(rootDir, namespace, queue string, cfg WALConfig) *QueueWAL {
	dir := filepath.Join(rootDir, namespace, queue)
	if err := os.MkdirAll(dir, 0755); err != nil {
		panic(fmt.Errorf("wal: create queue dir: %w", err))
	}

	segmentID := findLastSegmentID(dir)

	q := &QueueWAL{
		dir:            dir,
		cfg:            cfg,
		segmentID:      segmentID,
		ch:             make(chan Event, 8192),
		stop:           make(chan struct{}),
		done:           make(chan struct{}),
		snapshotSem:    make(chan struct{}, 1),
		logger:         newQueueLogger(namespace, queue),
		Namespace:      namespace,
		Queue:          queue,
		crcHash:        crc32.New(crcTable),
	}

	if err := q.openSegment(); err != nil {
		panic(fmt.Errorf("wal: open segment: %w", err))
	}

	go q.writerLoop()

	q.logger.Info("wal initialized", "dir", dir, "segment", segmentID)
	return q
}

// openSegment opens (or creates) the .log and .idx files for the current
// segmentID and wraps them in buffered writers.
func (q *QueueWAL) openSegment() error {
	logPath := filepath.Join(q.dir, fmt.Sprintf("%09d.log", q.segmentID))
	idxPath := filepath.Join(q.dir, fmt.Sprintf("%09d.idx", q.segmentID))

	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("wal: open segment: %w", err)
	}
	q.file = f

	stat, _ := f.Stat()
	q.fileSize = stat.Size()
	q.logBuf = bufio.NewWriterSize(f, 256*1024)

	idxf, err := os.OpenFile(idxPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("wal: open index: %w", err)
	}
	q.idxFile = idxf
	q.idxBuf = bufio.NewWriterSize(idxf, 64*1024)

	q.logger.Info("wal segment opened",
		"path", logPath,
		"bytes", q.fileSize,
		"segment", q.segmentID,
	)
	return nil
}

// rotateSegment rotates to a new segment file. Atomicity: new files are opened
// FIRST, then old files are closed (B5). This prevents a window where the WAL
// has no open segment file during rotation.
func (q *QueueWAL) rotateSegment() error {
	oldFile := q.file
	oldLogBuf := q.logBuf
	oldIdxFile := q.idxFile
	oldIdxBuf := q.idxBuf

	q.segmentID++
	q.fileSize = 0

	if err := q.openSegment(); err != nil {
		return err
	}

	// Close old files only after the new ones are open.
	if oldFile != nil {
		if err := oldLogBuf.Flush(); err != nil {
			return fmt.Errorf("wal: flush log buffer: %w", err)
		}
		if err := oldFile.Close(); err != nil {
			return fmt.Errorf("wal: close log segment: %w", err)
		}
	}
	if oldIdxFile != nil {
		if err := oldIdxBuf.Flush(); err != nil {
			return fmt.Errorf("wal: flush index buffer: %w", err)
		}
		if err := oldIdxFile.Close(); err != nil {
			return fmt.Errorf("wal: close index segment: %w", err)
		}
	}

	q.logger.Info("wal segment rotated", "segment", q.segmentID, "dir", q.dir)
	return nil
}

// removeOldSegments deletes segment files with ID < snapSegID - 1 (keeps the
// segment immediately preceding the snapshot as a safety margin). Called after
// a snapshot has been confirmed on disk. The WAL is the source of truth, but
// old segments that are fully captured by snapshots are safe to remove.
func (q *QueueWAL) removeOldSegments(snapSegID uint64) {
	keepFrom := uint64(0)
	if snapSegID > 1 {
		keepFrom = snapSegID - 1
	}
	entries, err := os.ReadDir(q.dir)
	if err != nil {
		q.logger.Warn("removeOldSegments: read dir", "error", err)
		return
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		var id uint64
		if _, err := fmt.Sscanf(e.Name(), "%d.", &id); err != nil || id >= keepFrom {
			continue
		}
		path := filepath.Join(q.dir, e.Name())
		if err := os.Remove(path); err != nil {
			q.logger.Warn("removeOldSegments: remove", "path", path, "error", err)
		}
	}
}

// findLastSegmentID scans dir for *.log files and returns the highest segment
// number found, or 0 if none exist.
func findLastSegmentID(dir string) uint64 {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}

	var maxID uint64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		var id uint64
		if _, err := fmt.Sscanf(e.Name(), "%d.log", &id); err == nil {
			if id > maxID {
				maxID = id
			}
		}
	}
	return maxID
}
