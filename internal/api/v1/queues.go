package v1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/danielgtaylor/huma/v2"

	"github.com/octarhq/octar/internal/broker"
	"github.com/octarhq/octar/internal/db"
	"github.com/octarhq/octar/internal/queue"
	"github.com/octarhq/octar/internal/scheduler"
)

// ── Input/output types ────────────────────────────────────────────────────────

// queueSummary is the lightweight list view — no stats, no groups.
// Calling Snapshot() on every queue to build a list would be O(queues × groups)
// and become unusable at scale.
type queueSummary struct {
	Name         string `json:"name"`
	Namespace    string `json:"namespace"`
	ActiveGroups int    `json:"active_groups"` // runtime group count (cheap: O(32 shards))
	ConfigCount  int    `json:"config_count"`  // declared configs count (O(1))
}

// queueDetail is the single-queue view returned by GET /queues/{ns}/{name}.
// Stats are intentionally excluded; use GET .../stats for runtime data.
type queueDetail struct {
	Name         string `json:"name"`
	Namespace    string `json:"namespace"`
	ActiveGroups int    `json:"active_groups"`
	ConfigCount  int    `json:"config_count"`
	Durable      bool   `json:"durable" doc:"Whether messages are fsynced to disk after each WAL flush"`
}

// pagedGroupStats is the response for the paginated stats endpoint.
type pagedGroupStats struct {
	Groups     []queue.GroupStats `json:"groups"`
	NextCursor string             `json:"next_cursor,omitempty"`
}

// pagedGroupConfigs is the response for the paginated group configs endpoint.
type pagedGroupConfigs struct {
	Configs    []queue.GroupConfig `json:"configs"`
	NextCursor string              `json:"next_cursor,omitempty"`
}

type rateLimitInput struct {
	Max    int    `json:"max" minimum:"1" doc:"Max messages per window"`
	Window string `json:"window" doc:"Duration string, e.g. '1s', '500ms', '1m'"`
}

type retryInput struct {
	MaxAttempts  int    `json:"max_attempts" minimum:"1"`
	Backoff      string `json:"backoff" enum:"fixed,linear,exponential"`
	InitialDelay string `json:"initial_delay" doc:"e.g. '1s'"`
	MaxDelay     string `json:"max_delay" doc:"e.g. '5m'"`
}

type dlqInput struct {
	Enabled bool   `json:"enabled"`
	Queue   string `json:"queue" doc:"Target DLQ queue name (same namespace)"`
}

type groupConfigInput struct {
	Parallelism  int             `json:"parallelism,omitempty" minimum:"1" default:"1" doc:"1 = sequential, N = parallel"`
	Quantum      int             `json:"quantum,omitempty" minimum:"1" default:"1" doc:"DRR quantum; 1 = strict round-robin, higher = weighted"`
	LeaseTimeout string          `json:"lease_timeout,omitempty" doc:"time before inflight msg returns to pending"`
	RateLimit    *rateLimitInput `json:"rate_limit,omitempty"`
	Retry        *retryInput     `json:"retry,omitempty"`
	DLQ          *dlqInput       `json:"dlq,omitempty"`
}

// ── Registration ──────────────────────────────────────────────────────────────

func registerQueues(humaAPI huma.API, store *db.Store, sched *scheduler.Scheduler, b *broker.Broker) {
	// ── GET /queues ─────────────────────────────────────────────────────────────
	// Lightweight list: no group stats, no config dump.
	// O(queues × 32 shards) — fast even with 10k groups per queue.
	huma.Register(humaAPI, huma.Operation{
		OperationID: "list-queues",
		Method:      http.MethodGet,
		Path:        "/queues",
		Summary:     "List all declared queues",
		Description: "Returns a lightweight summary for each queue. For runtime stats use GET /queues/{namespace}/{name}/stats.",
		Tags:        []string{"queues"},
		Security:    bearerAuth,
	}, func(ctx context.Context, _ *struct{}) (*struct{ Body []queueSummary }, error) {
		if _, err := requireAuth(ctx); err != nil {
			return nil, err
		}
		qs := sched.ListQueues()
		views := make([]queueSummary, len(qs))
		for i, q := range qs {
			views[i] = queueSummary{
				Name:         q.Name,
				Namespace:    q.Namespace,
				ActiveGroups: q.GroupCount(),
				ConfigCount:  q.ConfigCount(),
			}
		}
		return &struct{ Body []queueSummary }{Body: views}, nil
	})

	// ── POST /queues ────────────────────────────────────────────────────────────
	type declareInput struct {
		Body struct {
			Name      string `json:"name" minLength:"1" maxLength:"128" pattern:"^[a-z0-9_-]+$"`
			Namespace string `json:"namespace" minLength:"1" maxLength:"64"`
			// Durable controls whether messages in this queue are fsynced to disk
			// after each WAL flush batch, guaranteeing survival across power loss.
			// true  (default) → fsync; messages survive power loss; lower throughput.
			// false           → OS buffer only; 5–10x faster; messages survive process
			//                   crash but may be lost on power loss or kernel panic.
			Durable *bool `json:"durable,omitempty" doc:"fsync after each WAL flush (default: true). Set false for ephemeral/high-throughput queues."`
		}
	}
	huma.Register(humaAPI, huma.Operation{
		OperationID:   "declare-queue",
		Method:        http.MethodPost,
		Path:          "/queues",
		Summary:       "Declare a queue",
		Tags:          []string{"queues"},
		DefaultStatus: http.StatusCreated,
		Security:      bearerAuth,
	}, func(ctx context.Context, input *declareInput) (*struct{ Body queueDetail }, error) {
		if _, err := requireAuth(ctx); err != nil {
			return nil, err
		}
		if sched.GetQueue(input.Body.Namespace, input.Body.Name) != nil {
			return nil, huma.Error409Conflict("queue already exists")
		}

		ns, err := store.GetNamespace(input.Body.Namespace)
		if err != nil {
			return nil, dbError(err, "namespace")
		}

		// Resolve durable: explicit value or global default.
		durable := b.WAL.DefaultDurable()
		if input.Body.Durable != nil {
			durable = *input.Body.Durable
		}

		cfgJSON, _ := json.Marshal(map[string]any{"durable": durable})
		if _, err := store.CreateQueue(ns.ID, input.Body.Name, string(cfgJSON)); err != nil {
			return nil, dbError(err, "queue")
		}

		q := queue.NewQueue(input.Body.Name, input.Body.Namespace)
		sched.RegisterQueue(q)
		b.WAL.RegisterQueueState(input.Body.Namespace, input.Body.Name, func() interface{} {
			return q.ExportState()
		})
		b.WAL.SetQueueDurable(input.Body.Namespace, input.Body.Name, durable)
		return &struct{ Body queueDetail }{Body: toQueueDetail(q, durable)}, nil
	})

	// ── GET /queues/{namespace}/{name} ──────────────────────────────────────────
	type queuePathInput struct {
		Namespace string `path:"namespace"`
		Name      string `path:"name"`
	}
	huma.Register(humaAPI, huma.Operation{
		OperationID: "get-queue",
		Method:      http.MethodGet,
		Path:        "/queues/{namespace}/{name}",
		Summary:     "Get queue details",
		Description: "Returns queue metadata and group counts. For per-group stats use GET .../stats.",
		Tags:        []string{"queues"},
		Security:    bearerAuth,
	}, func(ctx context.Context, input *queuePathInput) (*struct{ Body queueDetail }, error) {
		if _, err := requireAuth(ctx); err != nil {
			return nil, err
		}
		q := sched.GetQueue(input.Namespace, input.Name)
		if q == nil {
			return nil, huma.Error404NotFound("queue not found")
		}
		durable := b.WAL.QueueDurable(input.Namespace, input.Name)
		return &struct{ Body queueDetail }{Body: toQueueDetail(q, durable)}, nil
	})

	// ── DELETE /queues/{namespace}/{name} ───────────────────────────────────────
	huma.Register(humaAPI, huma.Operation{
		OperationID:   "delete-queue",
		Method:        http.MethodDelete,
		Path:          "/queues/{namespace}/{name}",
		Summary:       "Delete a queue",
		Tags:          []string{"queues"},
		DefaultStatus: http.StatusNoContent,
		Security:      bearerAuth,
	}, func(ctx context.Context, input *queuePathInput) (*struct{}, error) {
		if _, err := requireAuth(ctx); err != nil {
			return nil, err
		}
		if sched.GetQueue(input.Namespace, input.Name) == nil {
			return nil, huma.Error404NotFound("queue not found")
		}

		if ns, err := store.GetNamespace(input.Namespace); err == nil {
			if err := store.DeleteQueue(ns.ID, input.Name); err != nil {
				return nil, dbError(err, "queue")
			}
		}

		sched.UnregisterQueue(input.Namespace, input.Name)
		// Clean up WAL segment files (B1).
		if err := b.WAL.DestroyQueue(input.Namespace, input.Name); err != nil {
			slog.Warn("failed to destroy queue WAL", "error", err, "namespace", input.Namespace, "queue", input.Name)
		}
		return nil, nil
	})

	// ── POST /queues/{namespace}/{name}/snapshot ────────────────────────────────
	// Triggers an immediate snapshot for the given queue. Useful before a
	// controlled shutdown to minimise recovery replay.
	huma.Register(humaAPI, huma.Operation{
		OperationID:   "trigger-snapshot",
		Method:        http.MethodPost,
		Path:          "/queues/{namespace}/{name}/snapshot",
		Summary:       "Trigger a manual snapshot",
		Tags:          []string{"queues"},
		DefaultStatus: http.StatusAccepted,
		Security:      bearerAuth,
	}, func(ctx context.Context, input *queuePathInput) (*struct {
		Body struct {
			Message string `json:"message"`
		}
	}, error) {
		if _, err := requireAuth(ctx); err != nil {
			return nil, err
		}
		if sched.GetQueue(input.Namespace, input.Name) == nil {
			return nil, huma.Error404NotFound("queue not found")
		}
		go b.TriggerSnapshot(input.Namespace, input.Name)
		return &struct {
			Body struct {
				Message string `json:"message"`
			}
		}{Body: struct {
			Message string `json:"message"`
		}{Message: "snapshot triggered"}}, nil
	})

	// ── GET /queues/{namespace}/{name}/stats ────────────────────────────────────
	// Paginated runtime stats — safe for 100k+ groups per queue.
	type statsInput struct {
		Namespace string `path:"namespace"`
		Name      string `path:"name"`
		After     string `query:"after" doc:"Cursor from previous page (last group key seen)"`
		Limit     int    `query:"limit" minimum:"1" maximum:"1000" default:"100" doc:"Max groups per page"`
	}
	huma.Register(humaAPI, huma.Operation{
		OperationID: "get-queue-stats",
		Method:      http.MethodGet,
		Path:        "/queues/{namespace}/{name}/stats",
		Summary:     "Get paginated runtime group stats",
		Description: "Returns pending/processing counts per active group. Use `after` (last key from previous page) for cursor pagination. Sorted by group key.",
		Tags:        []string{"queues"},
		Security:    bearerAuth,
	}, func(ctx context.Context, input *statsInput) (*struct{ Body pagedGroupStats }, error) {
		if _, err := requireAuth(ctx); err != nil {
			return nil, err
		}
		q := sched.GetQueue(input.Namespace, input.Name)
		if q == nil {
			return nil, huma.Error404NotFound("queue not found")
		}
		limit := input.Limit
		if limit <= 0 {
			limit = 100
		}
		groups, next := q.PageGroupStats(input.After, limit)
		return &struct{ Body pagedGroupStats }{Body: pagedGroupStats{
			Groups:     groups,
			NextCursor: next,
		}}, nil
	})

	// ── GET /queues/{namespace}/{name}/stats/{key} ──────────────────────────────
	// Single-group stats: O(1), touches only one shard.
	type singleStatInput struct {
		Namespace string `path:"namespace"`
		Name      string `path:"name"`
		Key       string `path:"key"`
	}
	huma.Register(humaAPI, huma.Operation{
		OperationID: "get-group-stats",
		Method:      http.MethodGet,
		Path:        "/queues/{namespace}/{name}/stats/{key}",
		Summary:     "Get runtime stats for a single group",
		Description: "O(1) — touches only the shard that owns the group. Use this for per-tenant dashboards instead of polling the full stats page.",
		Tags:        []string{"queues"},
		Security:    bearerAuth,
	}, func(ctx context.Context, input *singleStatInput) (*struct{ Body queue.GroupStats }, error) {
		if _, err := requireAuth(ctx); err != nil {
			return nil, err
		}
		q := sched.GetQueue(input.Namespace, input.Name)
		if q == nil {
			return nil, huma.Error404NotFound("queue not found")
		}
		stats, ok := q.GetGroupStats(input.Key)
		if !ok {
			// Group may not have received any messages yet; return zeroed stats.
			stats = queue.GroupStats{Key: input.Key}
		}
		return &struct{ Body queue.GroupStats }{Body: stats}, nil
	})

	// ── GET /queues/{namespace}/{name}/groups ───────────────────────────────────
	// Declared configs with cursor pagination.
	type listGroupsInput struct {
		Namespace string `path:"namespace"`
		Name      string `path:"name"`
		After     string `query:"after" doc:"Cursor from previous page"`
		Limit     int    `query:"limit" minimum:"1" maximum:"1000" default:"100" doc:"Max configs per page"`
	}
	huma.Register(humaAPI, huma.Operation{
		OperationID: "list-group-configs",
		Method:      http.MethodGet,
		Path:        "/queues/{namespace}/{name}/groups",
		Summary:     "List group configurations",
		Description: "Returns declared group configurations (exact keys and wildcards) with cursor pagination. Exact configs are returned in alphabetical order; wildcard configs follow.",
		Tags:        []string{"groups"},
		Security:    bearerAuth,
	}, func(ctx context.Context, input *listGroupsInput) (*struct{ Body pagedGroupConfigs }, error) {
		if _, err := requireAuth(ctx); err != nil {
			return nil, err
		}
		q := sched.GetQueue(input.Namespace, input.Name)
		if q == nil {
			return nil, huma.Error404NotFound("queue not found")
		}
		limit := input.Limit
		if limit <= 0 {
			limit = 100
		}
		configs, next := q.PageGroupConfigs(input.After, limit)
		return &struct{ Body pagedGroupConfigs }{Body: pagedGroupConfigs{
			Configs:    configs,
			NextCursor: next,
		}}, nil
	})

	// ── PUT /queues/{namespace}/{name}/groups/{key} ─────────────────────────────
	type setGroupInput struct {
		Namespace string `path:"namespace"`
		Name      string `path:"name"`
		Key       string `path:"key"`
		Body      groupConfigInput
	}
	huma.Register(humaAPI, huma.Operation{
		OperationID: "set-group-config",
		Method:      http.MethodPut,
		Path:        "/queues/{namespace}/{name}/groups/{key}",
		Summary:     "Create or update a group configuration",
		Description: "Upserts a group configuration. Supports wildcard keys like `group-*` or `tenant-*`. All consumers matching the pattern inherit this config.",
		Tags:        []string{"groups"},
		Security:    bearerAuth,
	}, func(ctx context.Context, input *setGroupInput) (*struct{ Body queue.GroupConfig }, error) {
		if _, err := requireAuth(ctx); err != nil {
			return nil, err
		}
		q := sched.GetQueue(input.Namespace, input.Name)
		if q == nil {
			return nil, huma.Error404NotFound("queue not found")
		}
		cfg, err := toGroupConfig(input.Key, input.Body)
		if err != nil {
			return nil, huma.Error422UnprocessableEntity(err.Error())
		}
		q.SetGroupConfig(cfg)

		if err := persistGroupConfig(store, input.Namespace, input.Name, cfg); err != nil {
			_ = err // non-fatal: in-memory is authoritative
		}

		return &struct{ Body queue.GroupConfig }{Body: cfg}, nil
	})

	// ── GET /queues/{namespace}/{name}/groups/{key} ─────────────────────────────
	type getGroupInput struct {
		Namespace string `path:"namespace"`
		Name      string `path:"name"`
		Key       string `path:"key"`
	}
	huma.Register(humaAPI, huma.Operation{
		OperationID: "get-group-config",
		Method:      http.MethodGet,
		Path:        "/queues/{namespace}/{name}/groups/{key}",
		Summary:     "Get a single group configuration",
		Description: "Returns the configuration for a specific group key (exact or wildcard).",
		Tags:        []string{"groups"},
		Security:    bearerAuth,
	}, func(ctx context.Context, input *getGroupInput) (*struct{ Body queue.GroupConfig }, error) {
		if _, err := requireAuth(ctx); err != nil {
			return nil, err
		}
		q := sched.GetQueue(input.Namespace, input.Name)
		if q == nil {
			return nil, huma.Error404NotFound("queue not found")
		}
		cfg, ok := q.GetGroupConfig(input.Key)
		if !ok {
			return nil, huma.Error404NotFound("group config not found")
		}
		return &struct{ Body queue.GroupConfig }{Body: cfg}, nil
	})

	// ── DELETE /queues/{namespace}/{name}/groups/{key} ──────────────────────────
	type deleteGroupInput struct {
		Namespace string `path:"namespace"`
		Name      string `path:"name"`
		Key       string `path:"key"`
	}
	huma.Register(humaAPI, huma.Operation{
		OperationID:   "delete-group-config",
		Method:        http.MethodDelete,
		Path:          "/queues/{namespace}/{name}/groups/{key}",
		Summary:       "Delete a group configuration",
		Description:   "Removes a group configuration. Active consumers fall back to the default config.",
		Tags:          []string{"groups"},
		DefaultStatus: http.StatusNoContent,
		Security:      bearerAuth,
	}, func(ctx context.Context, input *deleteGroupInput) (*struct{}, error) {
		if _, err := requireAuth(ctx); err != nil {
			return nil, err
		}
		q := sched.GetQueue(input.Namespace, input.Name)
		if q == nil {
			return nil, huma.Error404NotFound("queue not found")
		}
		if !q.DeleteGroupConfig(input.Key) {
			return nil, huma.Error404NotFound("group config not found")
		}

		if ns, err := store.GetNamespace(input.Namespace); err == nil {
			if dbQueue, err := store.GetQueueByName(ns.ID, input.Name); err == nil {
				_ = store.DeleteGroupConfig(dbQueue.ID, input.Key)
			}
		}

		return nil, nil
	})
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func toQueueDetail(q *queue.Queue, durable bool) queueDetail {
	return queueDetail{
		Name:         q.Name,
		Namespace:    q.Namespace,
		ActiveGroups: q.GroupCount(),
		ConfigCount:  q.ConfigCount(),
		Durable:      durable,
	}
}

// persistGroupConfig encodes cfg to JSON and upserts it into the db.
func persistGroupConfig(store *db.Store, namespace, queueName string, cfg queue.GroupConfig) error {
	ns, err := store.GetNamespace(namespace)
	if err != nil {
		return err
	}
	dbQueue, err := store.GetQueueByName(ns.ID, queueName)
	if err != nil {
		return err
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	return store.UpsertGroupConfig(dbQueue.ID, cfg.Key, string(data))
}

func toGroupConfig(key string, in groupConfigInput) (queue.GroupConfig, error) {
	cfg := queue.GroupConfig{
		Key:         key,
		Parallelism: in.Parallelism,
		Quantum:     in.Quantum,
	}
	if cfg.Parallelism <= 0 {
		cfg.Parallelism = 1
	}
	if cfg.Quantum <= 0 {
		cfg.Quantum = 1
	}
	if in.LeaseTimeout != "" {
		d, err := time.ParseDuration(in.LeaseTimeout)
		if err != nil {
			return cfg, fmt.Errorf("lease_timeout: %w", err)
		}
		cfg.LeaseTimeout = d
	} else {
		cfg.LeaseTimeout = 5 * time.Minute
	}

	if in.RateLimit != nil {
		window, err := time.ParseDuration(in.RateLimit.Window)
		if err != nil {
			return cfg, fmt.Errorf("rate_limit.window: %w", err)
		}
		cfg.RateLimit = &queue.RateLimitConfig{
			Max:    in.RateLimit.Max,
			Window: window,
		}
	}

	retry := queue.RetryConfig{
		MaxAttempts:  3,
		Backoff:      queue.BackoffExponential,
		InitialDelay: time.Second,
		MaxDelay:     5 * time.Minute,
	}
	if in.Retry != nil {
		retry.MaxAttempts = in.Retry.MaxAttempts
		retry.Backoff = queue.BackoffStrategy(in.Retry.Backoff)
		if in.Retry.InitialDelay != "" {
			d, err := time.ParseDuration(in.Retry.InitialDelay)
			if err != nil {
				return cfg, fmt.Errorf("retry.initial_delay: %w", err)
			}
			retry.InitialDelay = d
		}
		if in.Retry.MaxDelay != "" {
			d, err := time.ParseDuration(in.Retry.MaxDelay)
			if err != nil {
				return cfg, fmt.Errorf("retry.max_delay: %w", err)
			}
			retry.MaxDelay = d
		}
	}
	cfg.Retry = retry

	if in.DLQ != nil {
		if in.DLQ.Enabled && in.DLQ.Queue == "" {
			return cfg, errors.New("dlq.queue is required when dlq.enabled is true")
		}
		cfg.DLQ = &queue.DLQConfig{
			Enabled: in.DLQ.Enabled,
			Queue:   in.DLQ.Queue,
		}
	}

	return cfg, nil
}
