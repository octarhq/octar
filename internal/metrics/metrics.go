// Package metrics centralises all Prometheus instrumentation for OCTAR.
//
// File layout:
//
//	metrics.go — registry bootstrap (Register / Handler)
//	wal.go     — WAL, segment, snapshot, and recovery metrics
//	queue.go   — queue, message processing, scheduler, and dispatcher metrics
//	auth.go    — authentication and session metrics
package metrics

import (
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	registerOnce sync.Once
	registry     *prometheus.Registry
)

// Register builds the custom Prometheus registry and registers every metric
// defined in this package. Safe to call multiple times; executes only once.
func Register() {
	registerOnce.Do(func() {
		registry = prometheus.NewRegistry()
		registry.MustRegister(
			// WAL / storage
			WALAppendDuration,
			WALFlushDuration,
			WALFSyncDuration,
			WALBatchSize,
			WALQueueDepth,
			WALSegmentRotations,
			WALCorruptedRecords,
			WALDiskFull,
			WALErrored,
			// Snapshot / recovery
			SnapshotDuration,
			SnapshotFailures,
			RecoveryDuration,
			RecoveryReplayedEvents,
			// Queue / messaging
			PublishedTotal,
			QueueReadyMessages,
			QueueProcessingMessages,
			QueueDelayedMessages,
			MessageProcessingLatency,
			RetriesTotal,
			DeadLetterTotal,
			NACKTotal,
			SchedulerActivations,
			DispatcherDelivered,
			DispatcherFailed,
			// Auth
			AuthSuccessTotal,
			AuthFailureTotal,
			AuthTokenIssuedTotal,
			AuthTokenRefreshedTotal,
			AuthTokenRevokedTotal,
			ActiveConnsGauge,
			ActiveMessagesGauge,
			ConnRateLimitRejected,
			AuthAPIKeyCreatedTotal,
			AuthAPIKeyRevokedTotal,
			ActiveSessions,
			PermissionDeniedTotal,
		)
	})
}

// Handler returns the HTTP handler that serves the /metrics endpoint from the
// custom registry. Calls Register if it hasn't been called yet.
func Handler() http.Handler {
	if registry == nil {
		Register()
	}
	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
}
