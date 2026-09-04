package main

import (
	"context"
	"log"
	"os"

	"github.com/uptrace/bun/migrate"

	"urls_etl/internal/config"
	"urls_etl/internal/infra/db/postgresql"
	"urls_etl/migrations"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: go run ./cmd/migrate [init|up|down|status]")
	}

	ctx := context.Background()

	dbCfg := config.NewDBConfig()

	db, err := postgresql.NewConnection(ctx, dbCfg)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	migrator := migrate.NewMigrator(
		db,
		migrations.Migrations,
	)

	switch os.Args[1] {
	case "init":
		err = migrator.Init(ctx)

	case "up":
		_, err = migrator.Migrate(ctx)

	case "down":
		_, err = migrator.Rollback(ctx)

	case "status":
		ms, err := migrator.MigrationsWithStatus(ctx)
		if err != nil {
			log.Fatal(err)
		}

		for _, migration := range ms {
			log.Printf(
				"migration=%s applied=%v",
				migration.Name,
				migration.IsApplied(),
			)
		}

	default:
		log.Fatalf("unknown command: %s", os.Args[1])
	}

	if err != nil {
		log.Fatal(err)
	}

	log.Println("migrations completed successfully")
}
