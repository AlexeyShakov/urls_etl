package main

import (
	"log/slog"
	"net/http"
	"os"
	"urls_etl/internal/config"
	"urls_etl/internal/infra/mock_server"
)

func main() {
	logger := slog.New(
		slog.NewTextHandler(
			os.Stdout,
			&slog.HandlerOptions{
				Level: config.GetLogLevel(),
			},
		),
	)

	slog.SetDefault(logger)

	http.HandleFunc("/getItems", mock_server.GetItemsHandler)

	http.HandleFunc("/service1/fillItems", mock_server.FillItemsHandler)
	http.HandleFunc("/service2/scoreItems", mock_server.ScoreItemsHandler)
	http.HandleFunc("/service3/logItems", mock_server.LogItemsHandler)

	slog.Info("mock server started", "port", 8080)

	if err := http.ListenAndServe(":8080", nil); err != nil {
		slog.Error("mock server stopped", "err", err)
	}
}
