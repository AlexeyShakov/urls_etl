package domain

import "errors"

var (
	ErrStorageUnavailableRetryable    = errors.New("storage retryable error")
	ErrStorageUnavailableNonRetryable = errors.New("storage non-retryable error")

	ErrNotFound = errors.New("not found")
)

func IsRetryable(err error) bool {
	return errors.Is(err, ErrStorageUnavailableRetryable)
}
