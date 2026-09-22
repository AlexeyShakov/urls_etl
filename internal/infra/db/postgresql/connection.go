package postgresql

import (
	"context"
	"database/sql"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"

	"urls_etl/internal/config"
)

// NewConnection создает подключение к PostgreSQL через Bun.
//
// Bun использует database/sql, который управляет пулом соединений
// и безопасен для конкурентного использования.
// Поэтому *bun.DB нужно создать один раз и переиспользовать во всем приложении.
//
// В дальнейшем параметры пула, например максимальное количество
// открытых и простаивающих соединений, можно настроить через database/sql.
func NewConnection(ctx context.Context, cfg config.Config) (*bun.DB, error) {
	sqlDB := sql.OpenDB(
		pgdriver.NewConnector(
			pgdriver.WithDSN(cfg.DSN()),
		),
	)

	db := bun.NewDB(sqlDB, pgdialect.New())

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}

	return db, nil
}
