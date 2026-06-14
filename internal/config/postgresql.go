package config

import (
	"fmt"
	"os"

	"github.com/joho/godotenv"
)

// Config содержит настройки подключения к PostgreSQL.
type Config struct {
	Host     string
	Port     string
	Name     string
	User     string
	Password string
	SSLMode  string
}

// DSN возвращает строку подключения к PostgreSQL.
func (c Config) DSN() string {
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=%s",
		c.User,
		c.Password,
		c.Host,
		c.Port,
		c.Name,
		c.SSLMode,
	)
}

// NewDBConfig создает конфиг PostgreSQL из .env.
//
// Если переменной окружения нет, используется fallback.
func NewDBConfig() Config {
	_ = godotenv.Load()

	return Config{
		Host:     getEnv("DB_HOST", "localhost"),
		Port:     getEnv("DB_PORT", "5432"),
		Name:     getEnv("DB_NAME", "urls_etl"),
		User:     getEnv("DB_USER", "urls_etl"),
		Password: getEnv("DB_PASSWORD", "urls_etl"),
		SSLMode:  getEnv("DB_SSLMODE", "disable"),
	}
}

func getEnv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	return value
}
