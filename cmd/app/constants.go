package main

import "time"

// RequestChannelLen TODO Можно сделать переменной окружения
const (
	RequestChannelLen = 10
	WorkersCount      = 2
	WorkerChannelLen  = 10

	MaxConnsPerHost     = 10
	MaxIdleConns        = 100
	MaxIdleConnsPerHost = 10
	IdleConnTimeout     = 90 * time.Second

	RequestTimeout = 3 * time.Second
	MaxRetries     = 3
)
