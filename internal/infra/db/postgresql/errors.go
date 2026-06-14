package postgresql

import (
	"context"
	"errors"
	"urls_etl/internal/domain"

	"github.com/jackc/pgx/v5/pgconn"
)

func mapPostgresError(err error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return domain.ErrStorageUnavailableRetryable
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "40001", // serialization_failure
			"40P01", // deadlock_detected
			"53300", // too_many_connections
			"08006", // connection_failure
			"08003", // connection_does_not_exist
			"08000": // connection_exception
			return domain.ErrStorageUnavailableRetryable

		case "23503": // foreign_key_violation
			return domain.ErrInvalidData

		case "23505": // unique_violation
			return domain.ErrConflict

		case "23502": // not_null_violation
			return domain.ErrInvalidData
		}
	}

	return domain.ErrStorageUnavailableNonRetryable
}
