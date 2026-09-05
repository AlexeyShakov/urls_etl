package domain

import "errors"

var (
	ErrStorageUnavailableRetryable    = errors.New("storage retryable error")
	ErrStorageUnavailableNonRetryable = errors.New("storage non-retryable error")
)

func IsRetryable(err error) bool {
	return errors.Is(err, ErrStorageUnavailableRetryable)
}
