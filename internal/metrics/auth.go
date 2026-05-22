package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Authentication and session metrics.
var (
	AuthSuccessTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "octar_auth_success_total",
			Help: "Total successful authentication attempts",
		},
		[]string{"method", "namespace"},
	)

	AuthFailureTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "octar_auth_failure_total",
			Help: "Total failed authentication attempts",
		},
		[]string{"method", "namespace", "reason"},
	)

	AuthTokenIssuedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "octar_auth_token_issued_total",
			Help: "Total JWT tokens issued",
		},
	)

	AuthTokenRefreshedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "octar_auth_token_refreshed_total",
			Help: "Total JWT tokens refreshed",
		},
	)

	AuthTokenRevokedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "octar_auth_token_revoked_total",
			Help: "Total JWT tokens revoked",
		},
	)

	AuthAPIKeyCreatedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "octar_auth_api_key_created_total",
			Help: "Total API keys created",
		},
	)

	AuthAPIKeyRevokedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "octar_auth_api_key_revoked_total",
			Help: "Total API keys revoked",
		},
	)

	ActiveSessions = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "octar_auth_active_sessions",
			Help: "Current number of active authenticated sessions",
		},
	)

	PermissionDeniedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "octar_permission_denied_total",
			Help: "Total permission denied events",
		},
		[]string{"subject", "namespace", "permission"},
	)
)
