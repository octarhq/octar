package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// WAL, segment, snapshot, and recovery metrics.
var (
	WALAppendDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "octar_wal_append_duration_seconds",
			Help:    "WAL append (channel send) duration",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"namespace", "queue"},
	)

	WALFlushDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "octar_wal_flush_duration_seconds",
			Help:    "WAL flush duration (batch write + fsync)",
			Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1},
		},
		[]string{"namespace", "queue"},
	)

	WALFSyncDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "octar_wal_fsync_duration_seconds",
			Help:    "WAL fsync duration",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1},
		},
		[]string{"namespace", "queue"},
	)

	WALBatchSize = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "octar_wal_batch_size",
			Help:    "WAL batch size (events per flush)",
			Buckets: []float64{1, 10, 50, 100, 200, 500, 1000},
		},
		[]string{"namespace", "queue"},
	)

	WALErrored = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "octar_wal_errored",
			Help: "1 if the WAL writer has permanently failed, 0 otherwise",
		},
		[]string{"namespace", "queue"},
	)

	WALDiskFull = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "octar_wal_disk_full_total",
			Help: "Total disk full (ENOSPC) errors",
		},
		[]string{"namespace", "queue"},
	)

	WALQueueDepth = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "octar_wal_queue_depth",
			Help: "Pending events in WAL channel",
		},
		[]string{"namespace", "queue"},
	)

	WALSegmentRotations = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "octar_wal_segment_rotations_total",
			Help: "Total WAL segment rotations",
		},
		[]string{"namespace", "queue"},
	)

	WALCorruptedRecords = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "octar_wal_corrupted_records_total",
			Help: "Total corrupted WAL records (CRC mismatch)",
		},
		[]string{"namespace", "queue"},
	)

	SnapshotDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "octar_snapshot_duration_seconds",
			Help:    "Snapshot creation duration",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"namespace", "queue"},
	)

	SnapshotFailures = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "octar_snapshot_failures_total",
			Help: "Total snapshot failures",
		},
		[]string{"namespace", "queue"},
	)

	RecoveryDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "octar_recovery_duration_seconds",
			Help:    "Recovery duration per queue",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"namespace", "queue"},
	)

	RecoveryReplayedEvents = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "octar_recovery_replayed_events_total",
			Help: "Total events replayed during recovery",
		},
		[]string{"namespace", "queue"},
	)
)
