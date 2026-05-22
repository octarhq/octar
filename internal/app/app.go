// Package app wires the broker components together and manages process lifecycle.
package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	apiv1 "github.com/83codes/octar/internal/api/v1"
	"github.com/83codes/octar/internal/auth"
	"github.com/83codes/octar/internal/broker"
	"github.com/83codes/octar/internal/config"
	"github.com/83codes/octar/internal/db"
	"github.com/83codes/octar/internal/metrics"
)

// App is the top-level object. It owns the broker and API server and
// coordinates graceful startup and shutdown.
type App struct {
	Config    *config.Config
	DB        *db.Store
	Broker    *broker.Broker
	APIServer *apiv1.Server
	logger    *slog.Logger
}

// New bootstraps the database store and initialises all components.
func New(cfg *config.Config) (*App, error) {
	log := slog.Default().With("component", "app")

	if cfg.Auth.DefaultAdmin.Password == "" || cfg.Auth.DefaultAdmin.Password == "admin" {
		pw := generateRandomPassword(24)
		cfg.Auth.DefaultAdmin.Password = pw

		if err := os.MkdirAll(cfg.Storage.DataDir, 0700); err != nil {
			log.Warn("cannot create data dir for admin credentials", "error", err)
		} else {
			credPath := filepath.Join(cfg.Storage.DataDir, "admin_credentials.txt")
			credContent := fmt.Sprintf("username: %s\npassword: %s\n", cfg.Auth.DefaultAdmin.Username, pw)
			if err := os.WriteFile(credPath, []byte(credContent), 0600); err != nil {
				log.Warn("cannot save admin credentials", "path", credPath, "error", err)
			} else {
				log.Info("default admin credentials saved", "path", credPath)
			}
		}
	}

	store, err := db.New(cfg.Storage.DataDir, cfg.Auth.DefaultAdmin)
	if err != nil {
		return nil, err
	}

	authSvc := auth.NewService(cfg.Auth, store, cfg.Storage.DataDir)

	b, err := broker.New(cfg, store, authSvc)
	if err != nil {
		return nil, err
	}

	return &App{
		Config:    cfg,
		DB:        store,
		Broker:    b,
		APIServer: apiv1.NewServer(cfg.API.Port, store, authSvc, b.Scheduler, b),
		logger:    log,
	}, nil
}

func generateRandomPassword(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func (a *App) Start() error {
	metrics.Register()

	a.logger.Info("starting broker",
		"host", a.Config.Server.Host,
		"port", a.Config.Server.Port,
	)

	if err := a.Broker.Start(); err != nil {
		return err
	}

	go func() {
		if err := a.APIServer.Start(); err != nil {
			a.logger.Error("api server error", "error", err)
		}
	}()

	if a.Config.Metrics.Enabled {
		go a.serveMetrics()
	}

	if a.Config.PProf.Enabled {
		go a.servePProf()
	}

	a.logger.Info("broker started", "api_port", a.Config.API.Port)
	return a.waitForShutdown()
}

// serveMetrics exposes only the /metrics endpoint on its own isolated mux.
// It does NOT share http.DefaultServeMux, so pprof routes are never reachable here.
func (a *App) serveMetrics() {
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())

	addr := fmt.Sprintf(":%d", a.Config.Metrics.Port)
	a.logger.Info("metrics server listening", "addr", addr)

	if err := http.ListenAndServe(addr, mux); err != nil {
		a.logger.Error("metrics server error", "error", err)
	}
}

// servePProf exposes only pprof endpoints on its own isolated mux.
// Kept separate from the API server so profiling is never accidentally public.
func (a *App) servePProf() {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	addr := fmt.Sprintf(":%d", a.Config.PProf.Port)
	a.logger.Info("pprof server listening", "addr", addr)

	if err := http.ListenAndServe(addr, mux); err != nil {
		a.logger.Error("pprof server error", "error", err)
	}
}

func (a *App) waitForShutdown() error {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	a.logger.Info("shutdown signal received", "signal", sig)
	return a.Stop()
}

func (a *App) Stop() error {
	a.logger.Info("stopping octar broker")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := a.APIServer.Stop(ctx); err != nil {
		return err
	}
	if err := a.Broker.Stop(); err != nil {
		return err
	}

	a.logger.Info("octar broker stopped")
	return nil
}
