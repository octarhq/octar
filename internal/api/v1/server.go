package v1

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humachi"
	"github.com/go-chi/chi/v5"

	"github.com/83codes/octar/internal/auth"
	"github.com/83codes/octar/internal/broker"
	"github.com/83codes/octar/internal/db"
	"github.com/83codes/octar/internal/scheduler"
)

// bearerAuth is the security requirement applied to every protected operation.
var bearerAuth = []map[string][]string{{"BearerAuth": {}}}

// Server is the HTTP management API (control plane).
// Data-plane traffic flows over TCP — see internal/server/tcp_server.go.
type Server struct {
	db     *db.Store
	logger *slog.Logger
	http   *http.Server
	broker *broker.Broker
}

func NewServer(port int, store *db.Store, authSvc *auth.Service, sched *scheduler.Scheduler, b *broker.Broker) *Server {
	router := chi.NewRouter()

	router.Use(authMiddleware(authSvc))

	cfg := huma.DefaultConfig("OCTAR Management API", "0.2.0")
	cfg.Info.Description = strings.Join([]string{
		"REST control-plane for the OCTAR message broker.",
		"",
		"**Resources**",
		"- **namespaces** — logical isolation boundaries (CRUD)",
		"- **queues** — processing channels within a namespace (CRUD)",
		"- **groups** — consumer group configurations with fairness/retry/DLQ settings (CRUD)",
		"- **users** — operator accounts with RBAC roles (CRUD + per-namespace permissions)",
		"- **auth** — JWT login and token refresh",
		"- **api-keys** — long-lived API keys for services and integrations",
		"- **system** — health check, metrics, permission catalog",
	}, "\n")
	cfg.DocsPath = "" // replaced by Stoplight Elements below

	humaAPI := humachi.New(router, cfg)

	// Register Bearer JWT security scheme so Stoplight / Swagger UI shows the Authorize button.
	oa := humaAPI.OpenAPI()
	if oa.Components == nil {
		oa.Components = &huma.Components{}
	}
	if oa.Components.SecuritySchemes == nil {
		oa.Components.SecuritySchemes = map[string]*huma.SecurityScheme{}
	}
	oa.Components.SecuritySchemes["BearerAuth"] = &huma.SecurityScheme{
		Type:         "http",
		Scheme:       "bearer",
		BearerFormat: "JWT",
	}

	// Stoplight Elements — better "Try it" UX with built-in auth support.
	router.Get("/docs", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(stoplightHTML))
	})

	s := &Server{
		db:     store,
		logger: slog.Default().With("component", "api"),
		http:   &http.Server{Addr: fmt.Sprintf(":%d", port), Handler: router},
		broker: b,
	}

	registerAuth(humaAPI, store, authSvc)
	registerAPIKeys(humaAPI, authSvc)
	registerHealth(humaAPI, b)
	registerMetrics(humaAPI, sched)
	registerPermissions(humaAPI)
	registerNamespaces(humaAPI, store, authSvc)
	registerUsers(humaAPI, store)
	registerQueues(humaAPI, store, sched, b)

	return s
}

func (s *Server) Start() error {
	s.logger.Info("starting management API", "addr", s.http.Addr)
	if err := s.http.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (s *Server) Stop(ctx context.Context) error {
	return s.http.Shutdown(ctx)
}

const stoplightHTML = `<!doctype html>
<html>
  <head>
    <title>OCTAR Management API</title>
    <meta charset="utf-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <link rel="stylesheet" href="https://unpkg.com/@stoplight/elements/styles.min.css">
  </head>
  <body style="margin:0; height:100vh; overflow:hidden;">
    <elements-api
      apiDescriptionUrl="/openapi.json"
      router="hash"
      layout="sidebar"
    />
    <script src="https://unpkg.com/@stoplight/elements/web-components.min.js"></script>
  </body>
</html>`
