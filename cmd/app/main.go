package main

import (
	"context"
	"log"
	"sync"
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

	// Канал, куда кладется информация о внешних запросах. На схеме это next_channel
	requestChannel := make(chan domain.PipelineData, workerCfg.RequestChannelLen)
	// Каналы, куда складываются данные для запроса. На схеме это  in channel
	firstWorkerCh := make(chan domain.PipelineData, workerCfg.WorkerChannelLen)
	secondWorkerCh := make(chan domain.PipelineData, workerCfg.WorkerChannelLen)

	client := infra_http.NewHTTPClient(httpCfg)
	httpRequester := infra_http.NewHttpRequester(client, httpCfg)

	dbConfig := config.NewDBConfig()
	dbConnection, err := postgresql.NewConnection(context.Background(), dbConfig)
	if err != nil {
		log.Fatalf("No connection to db: %s", err)
	}
	repo := postgresql.NewRepo(dbConnection)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		domain.DispatchRequests(requestChannel, firstWorkerCh, secondWorkerCh)
	}()
	// TODO можно ли передачу каналов и кол-во воркеров сделать динамически?
	wg.Add(2)
	ctx := context.Background()
	go func() {
		defer wg.Done()
		domain.RequestWorker(ctx, 1, firstWorkerCh, httpRequester, repo)
	}()

	go func() {
		defer wg.Done()
		domain.RequestWorker(ctx, 2, secondWorkerCh, httpRequester, repo)
	}()

	domain.RunPipeline(context.Background(), urls, requestChannel, repo)
	wg.Wait()
}
