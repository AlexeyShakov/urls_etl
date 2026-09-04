package main

import (
	"context"
	"log"

	"golang.org/x/sync/errgroup"

	"urls_etl/internal/config"
	"urls_etl/internal/domain"
	"urls_etl/internal/infra/db/postgresql"
	infra_http "urls_etl/internal/infra/http"
)

var urls = []domain.RequestData{
	{
		URL: "http://localhost:8080/getItems",
		Headers: map[string]string{
			"Authorization": "Bearer token-1",
			"Content-Type":  "application/json",
		},
		Payload: `{"user_id": 1}`,
	},
	{
		URL: "http://localhost:8080/getItems",
		Headers: map[string]string{
			"Authorization": "Bearer token-2",
			"Content-Type":  "application/json",
		},
		Payload: `{"user_id": 100}`,
	},
}

func main() {
	workerCfg := config.NewDefaultWorkersConfig()
	httpCfg := config.NewDefaultHTTPConfig()

	// Канал, куда кладется информация о внешних запросах.
	// На схеме это next_channel.
	requestChannel := make(
		chan domain.PipelineData,
		workerCfg.RequestChannelLen,
	)

	// Каналы отдельных worker-ов.
	firstWorkerCh := make(
		chan domain.PipelineData,
		workerCfg.WorkerChannelLen,
	)
	secondWorkerCh := make(
		chan domain.PipelineData,
		workerCfg.WorkerChannelLen,
	)

	client := infra_http.NewHTTPClient(httpCfg)
	httpRequester := infra_http.NewHttpRequester(client, httpCfg)

	group, groupCtx := errgroup.WithContext(context.Background())

	dbConfig := config.NewDBConfig()

	dbConnection, err := postgresql.NewConnection(groupCtx, dbConfig)
	if err != nil {
		log.Fatalf("No connection to db: %s", err)
	}

	repo := postgresql.NewRepo(dbConnection)

	group.Go(func() error {
		domain.DispatchRequests(
			requestChannel,
			firstWorkerCh,
			secondWorkerCh,
		)

		return nil
	})

	// TODO: сделать количество worker-ов и их каналы динамическими.
	firstWorker := domain.NewRequestWorker(
		1,
		firstWorkerCh,
		httpRequester,
		repo,
	)
	secondWorker := domain.NewRequestWorker(
		2,
		secondWorkerCh,
		httpRequester,
		repo,
	)

	group.Go(func() error {
		firstWorker.Run(groupCtx)
		return nil
	})

	group.Go(func() error {
		secondWorker.Run(groupCtx)
		return nil
	})

	domain.RunPipeline(
		groupCtx,
		urls,
		requestChannel,
		repo,
	)

	if err := group.Wait(); err != nil {
		log.Printf("pipeline goroutine failed: %s", err)
	}
}
