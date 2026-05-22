package recovery

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/83codes/octar/internal/storage/snapshot"
	"log/slog"
)

type QueueRecoveryInfo struct {
	Namespace     string
	Queue         string
	Dir           string
	SnapshotPath  string
	SnapshotSeq   uint64
	SnapshotSegID uint64
}

type Bootstrap struct {
	rootDir string
	logger  *slog.Logger
}

func NewBootstrap(rootDir string) *Bootstrap {
	return &Bootstrap{
		rootDir: rootDir,
		logger:  slog.Default().With("component", "recovery", "module", "bootstrap"),
	}
}

func (b *Bootstrap) DiscoverQueues() ([]QueueRecoveryInfo, error) {
	entries, err := os.ReadDir(b.rootDir)
	if err != nil {
		return nil, fmt.Errorf("recovery: read wal root: %w", err)
	}

	var queues []QueueRecoveryInfo

	for _, ns := range entries {
		if !ns.IsDir() {
			continue
		}

		nsDir := filepath.Join(b.rootDir, ns.Name())
		queueEntries, err := os.ReadDir(nsDir)
		if err != nil {
			b.logger.Warn("cannot read namespace dir", "namespace", ns.Name(), "error", err)
			continue
		}

		for _, q := range queueEntries {
			if !q.IsDir() {
				continue
			}

			qDir := filepath.Join(nsDir, q.Name())
			snapPath, _, err := snapshot.FindLatestSnapshot(qDir)
			if err != nil {
				b.logger.Warn("failed to find snapshot", "queue", q.Name(), "error", err)
				continue
			}

			if snapPath == "" {
				b.logger.Info("no snapshot found, will replay from start", "namespace", ns.Name(), "queue", q.Name())
				queues = append(queues, QueueRecoveryInfo{
					Namespace:    ns.Name(),
					Queue:        q.Name(),
					Dir:          qDir,
					SnapshotPath: "",
					SnapshotSeq:  0,
					SnapshotSegID: 0,
				})
				continue
			}

			snap, err := b.loadSnapshotInfo(snapPath)
			if err != nil {
				b.logger.Warn("failed to load snapshot info", "path", snapPath, "error", err)
				queues = append(queues, QueueRecoveryInfo{
					Namespace:    ns.Name(),
					Queue:        q.Name(),
					Dir:          qDir,
					SnapshotPath: "",
					SnapshotSeq:  0,
					SnapshotSegID: 0,
				})
				continue
			}

			b.logger.Info("found snapshot", "namespace", ns.Name(), "queue", q.Name(),
				"segment", snap.SegmentID, "wal_seq", snap.WALSeq)

			queues = append(queues, QueueRecoveryInfo{
				Namespace:     ns.Name(),
				Queue:         q.Name(),
				Dir:           qDir,
				SnapshotPath:  snapPath,
				SnapshotSeq:   snap.WALSeq,
				SnapshotSegID: snap.SegmentID,
			})
		}
	}

	return queues, nil
}

func (b *Bootstrap) loadSnapshotInfo(path string) (*snapshot.Snapshot, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("recovery: open snapshot: %w", err)
	}
	defer f.Close()

	sr := snapshot.NewReader(f)
	return sr.Read()
}