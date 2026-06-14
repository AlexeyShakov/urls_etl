package postgresql

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"urls_etl/internal/config"
)

// NewConnection создает пул подключений к PostgreSQL.
//
// pgxpool.Pool безопасен для конкурентного использования,
// поэтому его нужно создать один раз и переиспользовать во всем приложении.
// В дальнейшем можно указать, сколько соединений мы хотим в пуле
func NewConnection(ctx context.Context, cfg config.Config) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, cfg.DSN())
	if err != nil {
		return nil, err
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}

	return pool, nil
}
