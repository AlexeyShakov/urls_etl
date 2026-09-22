package config

import "time"

type HTTPConfig struct {
	MaxConnsPerHost     int
	MaxIdleConns        int
	MaxIdleConnsPerHost int
	IdleConnTimeout     time.Duration

	RequestTimeout time.Duration
	MaxRetries     int
}

func NewDefaultHTTPConfig() HTTPConfig {
	return HTTPConfig{
		MaxConnsPerHost:     10,
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     90 * time.Second,

		RequestTimeout: 3 * time.Second,
		MaxRetries:     3,
	}
}
