package main

import (
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
	"github.com/joho/godotenv"
)

type Config struct {
	HTTP     HTTPConfig
	Postgres PostgresConfig
	Workers  WorkersConfig
	Logging  LoggingConfig
}

type HTTPConfig struct {
	MaxConnsPerHost     int           `env:"HTTP_MAX_CONNS_PER_HOST" env-default:"10"`
	MaxIdleConns        int           `env:"HTTP_MAX_IDLE_CONNS" env-default:"100"`
	MaxIdleConnsPerHost int           `env:"HTTP_MAX_IDLE_CONNS_PER_HOST" env-default:"10"`
	IdleConnTimeout     time.Duration `env:"HTTP_IDLE_CONN_TIMEOUT" env-default:"90s"`
	RequestTimeout      time.Duration `env:"HTTP_REQUEST_TIMEOUT" env-default:"3s"`
	MaxRetries          int           `env:"HTTP_MAX_RETRIES" env-default:"3"`
}

type PostgresConfig struct {
	Host     string `env:"DB_HOST" env-default:"localhost"`
	Port     string `env:"DB_PORT" env-default:"5432"`
	Name     string `env:"DB_NAME" env-default:"urls_etl"`
	User     string `env:"DB_USER" env-default:"urls_etl"`
	Password string `env:"DB_PASSWORD" env-default:"urls_etl"`
	SSLMode  string `env:"DB_SSLMODE" env-default:"disable"`
}

func (c PostgresConfig) DSN() string {
	u := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(c.User, c.Password),
		Host:   net.JoinHostPort(c.Host, c.Port),
		Path:   "/" + c.Name,
	}

	query := u.Query()
	query.Set("sslmode", c.SSLMode)
	u.RawQuery = query.Encode()

	return u.String()
}

type WorkersConfig struct {
	RequestChannelLen int `env:"REQUEST_CHANNEL_LEN" env-default:"10"`
	WorkerChannelLen  int `env:"WORKER_CHANNEL_LEN" env-default:"10"`
	WorkersCount      int `env:"WORKERS_COUNT" env-default:"2"`
}

type LoggingConfig struct {
	Level slog.Level `env:"LOG_LEVEL" env-default:"INFO"`
}

func loadConfig() (Config, error) {
	var cfg Config

	// Для локального запуска. Отсутствие .env допустимо.
	if err := godotenv.Load(); err != nil && !os.IsNotExist(err) {
		return cfg, fmt.Errorf("load .env: %w", err)
	}

	if err := cleanenv.ReadEnv(&cfg); err != nil {
		return cfg, fmt.Errorf("read config: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return cfg, fmt.Errorf("validate config: %w", err)
	}

	return cfg, nil
}

func (c Config) Validate() error {
	if c.Workers.WorkersCount <= 0 {
		return fmt.Errorf("WORKERS_COUNT must be greater than zero")
	}

	if c.Workers.RequestChannelLen < 0 || c.Workers.WorkerChannelLen < 0 {
		return fmt.Errorf("channel lengths must not be negative")
	}

	if c.HTTP.RequestTimeout <= 0 {
		return fmt.Errorf("HTTP_REQUEST_TIMEOUT must be greater than zero")
	}

	if c.HTTP.MaxRetries < 0 {
		return fmt.Errorf("HTTP_MAX_RETRIES must not be negative")
	}

	if c.HTTP.MaxConnsPerHost < 0 ||
		c.HTTP.MaxIdleConns < 0 ||
		c.HTTP.MaxIdleConnsPerHost < 0 ||
		c.HTTP.IdleConnTimeout < 0 {
		return fmt.Errorf("HTTP connection settings must not be negative")
	}

	if strings.TrimSpace(c.Postgres.Host) == "" {
		return fmt.Errorf("DB_HOST must not be empty")
	}

	return nil
}
