package config

import (
	"log/slog"
	"os"
	"strings"
)

func GetLogLevel() slog.Level {
	switch strings.ToUpper(os.Getenv("MOCK_SERVER_LOG_LEVEL")) {
	case "DEBUG":
		return slog.LevelDebug
	case "INFO":
		return slog.LevelInfo
	case "WARN":
		return slog.LevelWarn
	case "ERROR":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
