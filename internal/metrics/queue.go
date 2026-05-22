package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Queue, message processing, scheduler, and dispatcher metrics.
var (
	PublishedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "octar_published_total",
			Help: "Total published events",
		},
		[]string{"namespace", "queue", "group"},
	)

	QueueReadyMessages = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "octar_queue_ready_messages",
			Help: "Number of ready messages in queue",
		},
		[]string{"namespace", "queue", "group"},
	)

	QueueProcessingMessages = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "octar_queue_processing_messages",
			Help: "Number of in-flight messages in queue",
		},
		[]string{"namespace", "queue", "group"},
	)

	QueueDelayedMessages = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "octar_queue_delayed_messages",
			Help: "Number of delayed (retry) messages in queue",
		},
		[]string{"namespace", "queue", "group"},
	)

	MessageProcessingLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "octar_message_processing_seconds",
			Help:    "Time from publish to ACK (end-to-end latency)",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
		},
		[]string{"namespace", "queue", "group"},
	)

	RetriesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "octar_retries_total",
			Help: "Total retry attempts",
		},
		[]string{"namespace", "queue", "group"},
	)

	DeadLetterTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "octar_deadletter_total",
			Help: "Total messages sent to DLQ",
		},
		[]string{"namespace", "queue", "group"},
	)

	NACKTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "octar_nack_total",
			Help: "Total NACK events",
		},
		[]string{"namespace", "queue", "group"},
	)

	SchedulerActivations = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "octar_scheduler_activations_total",
			Help: "Total scheduler activations",
		},
		[]string{"namespace", "queue", "group"},
	)

	DispatcherDelivered = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "octar_dispatcher_delivered_total",
			Help: "Total messages delivered to consumers",
		},
		[]string{"namespace", "queue", "group"},
	)

	ActiveConnsGauge = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "octar_active_connections",
			Help: "Current number of active TCP connections",
		},
	)

	ActiveMessagesGauge = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "octar_active_messages",
			Help: "Current number of pending+processing messages system-wide",
		},
	)

	ConnRateLimitRejected = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "octar_conn_rate_limit_rejected_total",
			Help: "Total connections rejected by rate limiter",
		},
	)

	DispatcherFailed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "octar_dispatcher_failed_total",
			Help: "Total delivery failures (backpressure / no subscriber)",
		},
		[]string{"namespace", "queue", "group"},
	)
)
