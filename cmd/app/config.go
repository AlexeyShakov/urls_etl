package main

import (
	"os"

	"github.com/joho/godotenv"

	"urls_etl/internal/config"
)

func loadDBConfig() config.PostgresConfig {
	_ = godotenv.Load()

	return config.PostgresConfig{
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
