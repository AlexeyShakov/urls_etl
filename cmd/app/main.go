package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"urls_etl/internal/infra/shutdown"

	"golang.org/x/sync/errgroup"

	"urls_etl/internal/domain"
	"urls_etl/internal/infra/db/postgresql"
	"urls_etl/internal/infra/http_client"
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go watchShutdown(ctx, cancel, shutdown.OSSource{})

	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: cfg.Logging.Level,
	}))
	slog.SetDefault(logger)

	// Общий канал задач для выполнения HTTP-запросов.
	requestChannel := make(
		chan domain.PipelineData,
		cfg.Workers.RequestChannelLen,
	)

	// Входные каналы HTTP-воркеров.
	firstWorkerCh := make(
		chan domain.PipelineData,
		cfg.Workers.WorkerChannelLen,
	)

	secondWorkerCh := make(
		chan domain.PipelineData,
		cfg.Workers.WorkerChannelLen,
	)

	// Канал результатов HTTP-запросов.
	stageResultCh := make(
		chan domain.RequestResult,
		cfg.Workers.WorkerChannelLen,
	)

	client := http_client.NewHTTPClient(cfg.HTTP)
	httpRequester := http_client.NewHttpRequester(client, cfg.HTTP)

	dbConnection, err := postgresql.NewConnection(ctx, cfg.Postgres)
	if err != nil {
		log.Fatalf("no connection to db: %s", err)
	}
	defer dbConnection.Close()

	repo := postgresql.NewRepo(dbConnection)

	requestBuilders := []domain.ItemsRequestBuilder{
		domain.BuildFillItemsRequest,
		domain.BuildScoreItemsRequest,
		domain.BuildLogItemsRequest,
	}

	pipelineCoordinator := domain.NewPipelineCoordinator()

	firstRequestWorker := domain.NewRequestWorker(
		1,
		firstWorkerCh,
		httpRequester,
		stageResultCh,
	)

	secondRequestWorker := domain.NewRequestWorker(
		2,
		secondWorkerCh,
		httpRequester,
		stageResultCh,
	)

	stageResultWorker := domain.NewStageResultWorker(
		repo,
		stageResultCh,
		requestChannel,
		requestBuilders,
		pipelineCoordinator,
	)

	// Основная группа долгоживущих goroutine приложения.
	//
	group, groupCtx := errgroup.WithContext(ctx)

	// Отдельная группа нужна только для RequestWorker.
	//
	// Все RequestWorker являются producer-ами stageResultCh.
	// Поэтому stageResultCh можно закрыть только после завершения
	// всех HTTP-воркеров.
	requestWorkersGroup, requestWorkersCtx := errgroup.WithContext(groupCtx)

	group.Go(func() error {
		domain.DispatchRequests(
			groupCtx,
			requestChannel,
			firstWorkerCh,
			secondWorkerCh,
		)

		return nil
	})

	// TODO: Сделать количество requests-ов и их каналы динамическими.
	requestWorkersGroup.Go(func() error {
		firstRequestWorker.Run(requestWorkersCtx)
		return nil
	})

	requestWorkersGroup.Go(func() error {
		secondRequestWorker.Run(requestWorkersCtx)
		return nil
	})

	// Закрываем stageResultCh только после завершения всех его producer-ов.
	//
	// Эта goroutine входит в основную группу, потому что она связывает
	// жизненный цикл группы RequestWorker с остальной частью pipeline.
	group.Go(func() error {
		err := requestWorkersGroup.Wait()

		close(stageResultCh)

		return err
	})

	group.Go(func() error {
		stageResultWorker.Run(groupCtx)
		return nil
	})

	if runPipeErr := domain.RunPipeline(
		groupCtx,
		urls,
		requestChannel,
		repo,
		pipelineCoordinator,
	); runPipeErr != nil {
		slog.Error("RunPipeline failed", "err", runPipeErr)
	}

	if err := group.Wait(); err != nil {
		slog.Error("pipeline stopped with error", "err", err)
	}
}
