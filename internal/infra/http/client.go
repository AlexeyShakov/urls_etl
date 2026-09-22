package http

import (
	"net/http"
	"urls_etl/internal/config"
)

// NewHTTPClient создает переиспользуемый HTTP-клиент с настройками connection pool.
func NewHTTPClient(httpConfig config.HTTPConfig) *http.Client {
	transport := &http.Transport{
		// Максимальное количество одновременно открытых соединений к одному хосту.
		MaxConnsPerHost: httpConfig.MaxConnsPerHost,
		// Максимальное количество idle (неиспользуемых) keep-alive соединений для всех хостов.
		MaxIdleConns: httpConfig.MaxIdleConns,
		// Максимальное количество idle keep-alive соединений на один хост.
		MaxIdleConnsPerHost: httpConfig.MaxIdleConnsPerHost,
		// Через какое время неиспользуемое соединение будет закрыто.
		IdleConnTimeout: httpConfig.IdleConnTimeout,
	}

	return &http.Client{
		Transport: transport,
		Timeout:   httpConfig.RequestTimeout,
	}
}
