package logger

import (
	"log/slog"
	"os"
	"strings"

	"github.com/83codes/octar/internal/config"
)

// Init configures the global slog instance based on the LogConfig.
// It sets the log level and output format (JSON).
func Init(cfg config.LogConfig) {
	var levelVar slog.LevelVar
	levelVar.Set(parseLogLevel(cfg.Level))

	handler := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: &levelVar,
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			// Lowercase the level string (e.g. "INFO" -> "info")
			if a.Key == slog.LevelKey {
				level := a.Value.Any().(slog.Level)
				a.Value = slog.StringValue(strings.ToLower(level.String()))
			}
			return a
		},
	})
	
	slog.SetDefault(slog.New(handler))
}

func parseLogLevel(value string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "debug":
		return slog.LevelDebug
	case "info", "":
		return slog.LevelInfo
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
