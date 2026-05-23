package main

import (
	"log/slog"
	"os"

	"github.com/octarhq/octar/internal/app"
	"github.com/octarhq/octar/internal/config"
	"github.com/octarhq/octar/internal/logger"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load configuration", "error", err)
		os.Exit(1)
	}

	logger.Init(cfg.Log)

	application, err := app.New(cfg)
	if err != nil {
		slog.Error("failed to initialize application", "error", err)
		os.Exit(1)
	}

	if err := application.Start(); err != nil {
		slog.Error("application stopped with error", "error", err)
		os.Exit(1)
	}
}
