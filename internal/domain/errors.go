package domain

import "errors"

var (
	ErrStorageUnavailableRetryable    = errors.New("storage retryable error")
	ErrStorageUnavailableNonRetryable = errors.New("storage non-retryable error")

	ErrNotFound    = errors.New("not found")
	ErrConflict    = errors.New("conflict")
	ErrInvalidData = errors.New("invalid data")
)

func IsRetryable(err error) bool {
	return errors.Is(err, ErrStorageUnavailableRetryable)
}
