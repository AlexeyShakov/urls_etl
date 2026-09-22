package mock_server

import (
	"encoding/json"
	"log"
	"log/slog"
	"math/rand"
	"net/http"
)

// MaybeWriteRandomError случайно записывает HTTP-ошибку в response.
//
// Функция нужна, чтобы mock-сервер вел себя менее идеально:
// чаще всего возвращал успешный ответ, но иногда отдавал 4xx/5xx.
//
// Возвращает true, если ошибка была записана и handler должен завершиться.
// Возвращает false, если handler должен продолжить обычную обработку.
func MaybeWriteRandomError(w http.ResponseWriter) bool {
	roll := rand.Intn(100)

	switch {
	case roll < 90:
		// 90% успешных ответов
		return false

	case roll < 95:
		// 5% non-retryable ошибок
		status := NonRetryableStatuses[rand.Intn(len(NonRetryableStatuses))]
		writeErrorResponse(w, status)
		return true

	default:
		// 5% retryable ошибок
		status := RetryableStatuses[rand.Intn(len(RetryableStatuses))]
		writeErrorResponse(w, status)
		return true
	}
}

// writeErrorResponse записывает JSON-ответ с HTTP-ошибкой.
//
// В ответ добавляется:
// - текст ошибки;
// - статус-код;
// - признак retryable.
func writeErrorResponse(w http.ResponseWriter, status int) {
	slog.Debug(
		"mock server returns error",
		"status_code", status,
		"retryable", isRetryableStatus(status),
	)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	if err := json.NewEncoder(w).Encode(map[string]any{
		"error":     http.StatusText(status),
		"status":    status,
		"retryable": isRetryableStatus(status),
	}); err != nil {
		log.Printf("failed to encode error response: %v", err)
	}
}

func isRetryableStatus(status int) bool {
	_, ok := RetryableStatusSet[status]
	return ok
}
