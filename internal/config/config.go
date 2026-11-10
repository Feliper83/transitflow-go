// Package config provides small helpers for reading configuration from the
// environment with sensible defaults.
package config

import (
	"log/slog"
	"os"
	"strconv"
)

// Logger builds a JSON structured logger tagged with the service name.
func Logger(service string) *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", service)
}

// String returns the environment variable value or def if unset/empty.
func String(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// Float returns the environment variable parsed as a float64, or def if unset
// or unparseable.
func Float(key string, def float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return def
	}
	return f
}
