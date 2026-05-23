package recovery

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"log/slog"

	"github.com/octarhq/octar/internal/storage"
	"github.com/octarhq/octar/internal/storage/snapshot"
)

// ReplayHandler receives WAL events during a plain segment replay.
type ReplayHandler interface {
	OnPublish(event storage.Event)
	OnLease(event storage.Event)
	OnACK(event storage.Event)
	OnNACK(event storage.Event)
	OnExpire(event storage.Event)
}

// SnapshotHandler extends ReplayHandler with an initial snapshot callback,
// used when replaying from a snapshot checkpoint.
type SnapshotHandler interface {
	ReplayHandler
	OnSnapshot(snap *snapshot.Snapshot)
}

// Replay drives WAL segment replay for queue recovery.
type Replay struct {
	logger *slog.Logger
}

func NewReplay() *Replay {
	return &Replay{
		logger: slog.Default().With("component", "recovery", "module", "replay"),
	}
}

// ReplaySegment replays all .log segments in dir, skipping events with
// Seq < startSeq (already covered by a snapshot).
func (r *Replay) ReplaySegment(dir string, startSeq uint64, handler ReplayHandler) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("replay: read dir: %w", err)
	}

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		var segID uint64
		if _, err := fmt.Sscanf(e.Name(), "%d.log", &segID); err != nil {
			continue
		}
		logPath := filepath.Join(dir, e.Name())
		if err := r.replayFile(logPath, startSeq, handler); err != nil {
			r.logger.Warn("replay failed for segment", "path", logPath, "error", err)
		}
	}
	return nil
}

// ReplayFromSnapshot loads the snapshot (if any), then replays WAL events
// from the snapshot's checkpoint forward.
func (r *Replay) ReplayFromSnapshot(info QueueRecoveryInfo, handler SnapshotHandler) error {
	r.logger.Info("starting replay",
		"dir", info.Dir,
		"snapshot_seq", info.SnapshotSeq,
	)

	if info.SnapshotPath != "" {
		snap, err := r.tryLoadSnapshot(info.SnapshotPath)
		if err == nil && snap != nil {
			r.logger.Info("loaded snapshot", "groups", len(snap.Groups), "seq", snap.WALSeq)
			handler.OnSnapshot(snap)
		} else if err != nil {
			r.logger.Warn("all snapshot attempts failed, replaying from start", "error", err)
		}
	}

	startSeq := info.SnapshotSeq + 1
	entries, err := os.ReadDir(info.Dir)
	if err != nil {
		return fmt.Errorf("replay: read dir: %w", err)
	}

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		var segID uint64
		if _, err := fmt.Sscanf(e.Name(), "%d.log", &segID); err != nil {
			continue
		}
		if info.SnapshotPath != "" && segID < info.SnapshotSegID {
			continue
		}
		logPath := filepath.Join(info.Dir, e.Name())
		if err := r.replayFile(logPath, startSeq, handler); err != nil {
			r.logger.Warn("replay failed", "path", logPath, "error", err)
		}
	}

	r.logger.Info("replay complete", "dir", info.Dir)
	return nil
}

func (r *Replay) loadSnapshot(path string) (*snapshot.Snapshot, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("snapshot: open: %w", err)
	}
	defer f.Close()
	return snapshot.NewReader(f).Read()
}

// tryLoadSnapshot tries the primary snapshot path first. If that fails, it
// attempts N-1 (the previous snap file), and if that also fails it returns nil
// (no snapshot). This provides a graceful fallback against corrupt snapshots
// without blocking startup (C1).
func (r *Replay) tryLoadSnapshot(path string) (*snapshot.Snapshot, error) {
	snap, err := r.loadSnapshot(path)
	if err == nil {
		return snap, nil
	}
	r.logger.Warn("primary snapshot failed, trying N-1",
		"path", path, "error", err)

	// Derive the N-1 path: e.g. "000000005.snap" → "000000004.snap"
	dir := filepath.Dir(path)
	base := filepath.Base(path)
	var snapID uint64
	if _, e := fmt.Sscanf(base, "%d.snap", &snapID); e != nil || snapID == 0 {
		return nil, nil
	}
	fallbackPath := filepath.Join(dir, fmt.Sprintf("%09d.snap", snapID-1))
	fallback, err2 := r.loadSnapshot(fallbackPath)
	if err2 == nil {
		r.logger.Info("loaded N-1 snapshot", "path", fallbackPath)
		return fallback, nil
	}
	r.logger.Warn("N-1 snapshot also failed, proceeding without snapshot",
		"path", fallbackPath, "error", err2)
	return nil, nil
}

// replayFile opens a single .log segment and dispatches each event with
// Seq >= startSeq to handler. Uses storage.ReadEvent — the single source
// of truth for WAL record decoding (no duplication, CRC is always checked).
func (r *Replay) replayFile(path string, startSeq uint64, handler ReplayHandler) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("replay: open: %w", err)
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	for {
		event, err := storage.ReadEvent(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			r.logger.Warn("replay: read error", "path", path, "error", err)
			break
		}
		if event.Seq < startSeq {
			continue
		}
		r.dispatch(event, handler)
	}
	return nil
}

func (r *Replay) dispatch(event storage.Event, handler ReplayHandler) {
	switch event.Type {
	case storage.EventPublish:
		handler.OnPublish(event)
	case storage.EventLease:
		handler.OnLease(event)
	case storage.EventACK:
		handler.OnACK(event)
	case storage.EventNACK:
		handler.OnNACK(event)
	case storage.EventExpire:
		handler.OnExpire(event)
	}
}
