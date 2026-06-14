package main

import (
	"log"
	"os"

	"github.com/golang-migrate/migrate/v4"

	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"

	"urls_etl/internal/config"
)

// todo стоить заменить на специпльную библиотеку migrate
func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: go run ./cmd/migrate [up|down|version]")
	}

	dbCfg := config.NewDBConfig()

	m, err := migrate.New(
		"file://migrations",
		dbCfg.DSN(),
	)
	if err != nil {
		log.Fatal(err)
	}

	switch os.Args[1] {
	case "up":
		err = m.Up()
	case "down":
		err = m.Steps(-1)
	case "version":
		version, dirty, err := m.Version()
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("version=%d dirty=%v", version, dirty)
		return
	default:
		log.Fatalf("unknown command: %s", os.Args[1])
	}

	if err != nil && err != migrate.ErrNoChange {
		log.Fatal(err)
	}

	log.Println("migrations completed successfully")
}
