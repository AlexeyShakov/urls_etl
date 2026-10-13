package postgresql

import (
	"context"
	"errors"
	"fmt"
	"urls_etl/internal/domain/pipeline"

	"github.com/jackc/pgerrcode"
	"github.com/uptrace/bun/driver/pgdriver"
)

func mapPostgresError(err error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf(
			"%w: %v",
			pipeline.ErrStorageUnavailableRetryable,
			err,
		)
	}

	var pgErr pgdriver.Error
	if errors.As(err, &pgErr) {
		switch pgErr.Field('C') {
		case pgerrcode.SerializationFailure,
			pgerrcode.DeadlockDetected,
			pgerrcode.TooManyConnections,
			pgerrcode.ConnectionFailure,
			pgerrcode.ConnectionDoesNotExist,
			pgerrcode.ConnectionException:

			return fmt.Errorf(
				"%w: %v",
				pipeline.ErrStorageUnavailableRetryable,
				err,
			)
		}
	}

	return fmt.Errorf(
		"%w: %v",
		pipeline.ErrStorageUnavailableNonRetryable,
		err,
	)
}
