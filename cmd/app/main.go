package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"golang.org/x/sync/errgroup"

	"urls_etl/internal/domain_new/pipeline"
	"urls_etl/internal/domain_new/requests"
	"urls_etl/internal/domain_new/stage"
	"urls_etl/internal/infra/db/postgresql"
	"urls_etl/internal/infra/http_client"
	"urls_etl/internal/infra/shutdown"
)

var urls = []pipeline.RequestData{
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

// main запускает приложение и задает код завершения.
func main() {
	if err := run(); err != nil {
		slog.Error("application failed", "err", err)
		os.Exit(1)
	}
}

// run создает зависимости, запускает пайплайн и ожидает завершения воркеров.
func run() error {
	cfg, err := loadConfig()
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: cfg.Logging.Level,
	}))
	slog.SetDefault(logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go watchShutdown(ctx, cancel, shutdown.OSSource{})

	client := http_client.NewHTTPClient(cfg.HTTP)
	defer client.CloseIdleConnections()

	httpRequester := http_client.NewHttpRequester(client, cfg.HTTP)

	dbConnection, err := postgresql.NewConnection(ctx, cfg.Postgres)
	if err != nil {
		return fmt.Errorf("connect to PostgreSQL: %w", err)
	}
	defer func() {
		if err := dbConnection.Close(); err != nil {
			slog.Error("failed to close database connection", "err", err)
		}
	}()

	repo := postgresql.NewRepo(dbConnection)

	requestChannel := make(
		chan pipeline.PipelineData,
		cfg.Workers.RequestChannelLen,
	)

	firstWorkerCh := make(
		chan pipeline.PipelineData,
		cfg.Workers.WorkerChannelLen,
	)

	secondWorkerCh := make(
		chan pipeline.PipelineData,
		cfg.Workers.WorkerChannelLen,
	)

	stageResultCh := make(
		chan pipeline.RequestResult,
		cfg.Workers.WorkerChannelLen,
	)

	requestBuilders := []stage.ItemsRequestBuilder{
		stage.BuildFillItemsRequest,
		stage.BuildScoreItemsRequest,
		stage.BuildLogItemsRequest,
	}

	coordinator := pipeline.NewPipelineCoordinator()

	firstRequestWorker := requests.NewRequestWorker(
		1,
		firstWorkerCh,
		httpRequester,
		stageResultCh,
	)

	secondRequestWorker := requests.NewRequestWorker(
		2,
		secondWorkerCh,
		httpRequester,
		stageResultCh,
	)

	stageResultWorker := stage.NewStageResultWorker(
		repo,
		stageResultCh,
		requestChannel,
		requestBuilders,
		coordinator,
	)

	group, groupCtx := errgroup.WithContext(ctx)

	// Отдельная группа отслеживает всех отправителей результатов.
	requestWorkersGroup, requestWorkersCtx := errgroup.WithContext(groupCtx)

	group.Go(func() error {
		requests.DispatchRequests(
			groupCtx,
			requestChannel,
			firstWorkerCh,
			secondWorkerCh,
		)
		return nil
	})

	// TODO: Сделать количество воркеров и их каналы динамическими.
	requestWorkersGroup.Go(func() error {
		firstRequestWorker.Run(requestWorkersCtx)
		return nil
	})

	requestWorkersGroup.Go(func() error {
		secondRequestWorker.Run(requestWorkersCtx)
		return nil
	})

	group.Go(func() error {
		err := requestWorkersGroup.Wait()

		// Закрываем канал после завершения всех его отправителей.
		close(stageResultCh)

		return err
	})

	group.Go(func() error {
		stageResultWorker.Run(groupCtx)
		return nil
	})

	runPipeErr := pipeline.RunPipeline(
		groupCtx,
		urls,
		requestChannel,
		repo,
		coordinator,
	)
	if runPipeErr != nil {
		// Останавливаем воркеры перед ожиданием их завершения.
		cancel()
	}

	groupErr := group.Wait()

	return errors.Join(runPipeErr, groupErr)
}
