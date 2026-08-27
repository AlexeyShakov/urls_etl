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
	ctx := context.Background()

	workerCfg := config.NewDefaultWorkersConfig()
	httpCfg := config.NewDefaultHTTPConfig()

	// Общий канал задач для выполнения HTTP-запросов.
	requestChannel := make(
		chan domain.PipelineData,
		workerCfg.RequestChannelLen,
	)

	// Входные каналы HTTP-воркеров.
	firstWorkerCh := make(
		chan domain.PipelineData,
		workerCfg.WorkerChannelLen,
	)
	secondWorkerCh := make(
		chan domain.PipelineData,
		workerCfg.WorkerChannelLen,
	)

	// Канал результатов HTTP-запросов.
	stageResultCh := make(
		chan domain.RequestResult,
		workerCfg.WorkerChannelLen,
	)

	client := infra_http.NewHTTPClient(httpCfg)
	httpRequester := infra_http.NewHttpRequester(client, httpCfg)

	dbConfig := config.NewDBConfig()
	dbConnection, err := postgresql.NewConnection(ctx, dbConfig)
	if err != nil {
		log.Fatalf("no connection to db: %s", err)
	}

	repo := postgresql.NewRepo(dbConnection)

	requestBuilders := []domain.ItemsRequestBuilder{
		domain.BuildFillItemsRequest,
		domain.BuildScoreItemsRequest,
		domain.BuildLogItemsRequest,
	}

	pipelineCoordinator := domain.NewPipelineCoordinator()

	var wg sync.WaitGroup

	// Отдельный WaitGroup нужен только для HTTP-воркеров.
	//
	// Все HTTP-воркеры являются producer-ами общего stageResultCh.
	// Поэтому ни один отдельный worker не может безопасно закрыть этот канал.
	// stageResultCh можно закрыть только после завершения ВСЕХ RequestWorker.
	var requestWorkersWG sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		domain.DispatchRequests(
			requestChannel,
			firstWorkerCh,
			secondWorkerCh,
		)
	}()

	// TODO: Сделать количество worker-ов и их каналы динамическими.
	requestWorkersWG.Add(2)
	wg.Add(2)

	go func() {
		defer wg.Done()
		defer requestWorkersWG.Done()

		domain.RequestWorker(
			ctx,
			1,
			firstWorkerCh,
			httpRequester,
			stageResultCh,
		)
	}()

	go func() {
		defer wg.Done()
		defer requestWorkersWG.Done()

		domain.RequestWorker(
			ctx,
			2,
			secondWorkerCh,
			httpRequester,
			stageResultCh,
		)
	}()

	// Закрываем stageResultCh только после завершения всех его producer-ов(RequestWorker).
	//
	// Эта goroutine не добавляется в основной wg, потому что его задача —
	// только дождаться RequestWorker и закрыть канал. После закрытия канала
	// HandleStageResult завершит свой range и основной wg дождется его.
	go func() {
		requestWorkersWG.Wait()
		close(stageResultCh)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		domain.HandleStageResult(
			ctx,
			repo,
			stageResultCh,
			requestChannel,
			requestBuilders,
			pipelineCoordinator,
		)
	}()

	domain.RunPipeline(
		ctx,
		urls,
		requestChannel,
		repo,
		pipelineCoordinator,
	)

	// Ждем завершения Dispatcher, RequestWorker и HandleStageResult.
	wg.Wait()
}
