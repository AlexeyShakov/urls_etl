package pipeline

import "errors"

var (
	ErrStorageUnavailableRetryable = errors.New(
		"storage retryable error",
	)
	ErrStorageUnavailableNonRetryable = errors.New(
		"storage non-retryable error",
	)
	ErrNotFound = errors.New("not found")
)

// IsRetryable определяет, допускает ли ошибка хранилища повторную попытку.
func IsRetryable(err error) bool {
	return errors.Is(err, ErrStorageUnavailableRetryable)
}
