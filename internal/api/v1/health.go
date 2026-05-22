package v1

import (
	"context"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	"github.com/83codes/octar/internal/broker"
)

type healthOutput struct {
	Body struct {
		Status  string `json:"status" example:"ok"`
		Version string `json:"version" example:"0.1.0"`
		Checks  []checkResult `json:"checks,omitempty"`
	}
}

type checkResult struct {
	Name   string `json:"name"`
	Status string `json:"status"` // "ok" or "error"
	Detail string `json:"detail,omitempty"`
}

func registerHealth(api huma.API, b *broker.Broker) {
	huma.Register(api, huma.Operation{
		OperationID: "get-health",
		Method:      http.MethodGet,
		Path:        "/health",
		Summary:     "Health check",
		Tags:        []string{"system"},
	}, func(_ context.Context, _ *struct{}) (*healthOutput, error) {
		out := &healthOutput{}
		out.Body.Version = "0.1.0"

		var checks []checkResult

		// WAL health
		walErr := b.WAL.Err()
		if walErr == nil {
			checks = append(checks, checkResult{Name: "wal", Status: "ok"})
		} else {
			checks = append(checks, checkResult{Name: "wal", Status: "error", Detail: walErr.Error()})
		}

		// Scheduler health
		schedOK := b.Scheduler != nil
		if schedOK {
			checks = append(checks, checkResult{Name: "scheduler", Status: "ok"})
		} else {
			checks = append(checks, checkResult{Name: "scheduler", Status: "error", Detail: "scheduler not initialized"})
		}

		// Disk space (data dir)
		diskOK, diskDetail := b.CheckDiskSpace()
		if diskOK {
			checks = append(checks, checkResult{Name: "disk", Status: "ok"})
		} else {
			checks = append(checks, checkResult{Name: "disk", Status: "error", Detail: diskDetail})
		}

		// Aggregate status
		allOK := true
		for _, c := range checks {
			if c.Status != "ok" {
				allOK = false
				break
			}
		}
		if allOK {
			out.Body.Status = "ok"
		} else {
			out.Body.Status = "degraded"
		}
		out.Body.Checks = checks
		return out, nil
	})
}
