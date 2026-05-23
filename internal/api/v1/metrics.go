package v1

import (
	"context"
	"net/http"
	"time"

	"github.com/danielgtaylor/huma/v2"

	"github.com/octarhq/octar/internal/scheduler"
)

type metricsOutput struct {
	Body struct {
		GeneratedAt time.Time                `json:"generated_at"`
		Scheduler   scheduler.SchedulerStats `json:"scheduler"`
	}
}

func registerMetrics(api huma.API, sched *scheduler.Scheduler) {
	huma.Register(api, huma.Operation{
		OperationID: "get-internal-metrics",
		Method:      http.MethodGet,
		Path:        "/internal/metrics",
		Summary:     "Internal scheduler metrics (JSON)",
		Tags:        []string{"system"},
		Security:    bearerAuth,
	}, func(_ context.Context, _ *struct{}) (*metricsOutput, error) {
		out := &metricsOutput{}
		out.Body.GeneratedAt = time.Now().UTC()
		out.Body.Scheduler = sched.Metrics()
		return out, nil
	})
}
