package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"golang.org/x/sync/errgroup"

	"urls_etl/internal/config"
	"urls_etl/internal/domain/pipeline"
	"urls_etl/internal/domain/requests"
	"urls_etl/internal/domain/stages"
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

// run создает зависимости, запускает воркеры и передает управление Runner.
func run() error {
	workerCfg := config.NewDefaultWorkersConfig()
	httpCfg := config.NewDefaultHTTPConfig()

	// Загружаем .env до чтения уровня логирования.
	dbCfg := loadDBConfig()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: config.GetLogLevel(),
	}))
	slog.SetDefault(logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go watchShutdown(ctx, cancel, shutdown.OSSource{})

	client := http_client.NewHTTPClient(httpCfg)
	defer client.CloseIdleConnections()

	httpRequester := http_client.NewHttpRequester(client, httpCfg)

	dbConnection, err := postgresql.NewConnection(ctx, dbCfg)
	if err != nil {
		return fmt.Errorf("connect to PostgreSQL: %w", err)
	}
	defer func() {
		if err := dbConnection.Close(); err != nil {
			slog.Error("failed to close database connection", "err", err)
		}
	}()

	repo := postgresql.NewRepo(dbConnection)

	// Каналы обработки GetItems.
	getItemsReqCh := make(chan pipeline.PipelineData, workerCfg.ChannelsLen)
	firstReqWorkerCh := make(chan pipeline.PipelineData, workerCfg.ChannelsLen)
	secondReqWorkerCh := make(chan pipeline.PipelineData, workerCfg.ChannelsLen)
	getItemsResCh := make(chan pipeline.RequestResult, workerCfg.ChannelsLen)

	// Общие каналы downstream-воркеров.
	downstreamReqCh := make(chan pipeline.PipelineData, workerCfg.ChannelsLen)
	downstreamResCh := make(chan pipeline.RequestResult, workerCfg.ChannelsLen)

	getItemsCoordinator := requests.NewChannelCoordinator(
		2,
		getItemsResCh,
	)

	downstreamCoordinator := requests.NewChannelCoordinator(
		2,
		downstreamResCh,
	)

	// todo надо подумать, как сделать создание воркеров динамически
	firstGetItemsWorker := requests.NewRequestWorker(
		1,
		firstReqWorkerCh,
		httpRequester,
		getItemsResCh,
	)

	secondGetItemsWorker := requests.NewRequestWorker(
		2,
		secondReqWorkerCh,
		httpRequester,
		getItemsResCh,
	)

	firstDownstreamWorker := requests.NewRequestWorker(
		3,
		downstreamReqCh,
		httpRequester,
		downstreamResCh,
	)

	secondDownstreamWorker := requests.NewRequestWorker(
		4,
		downstreamReqCh,
		httpRequester,
		downstreamResCh,
	)

	requestBuilders := []stages.ItemsRequestBuilder{
		stages.BuildFillItemsRequest,
		stages.BuildScoreItemsRequest,
		stages.BuildLogItemsRequest,
	}

	getItemsResultWorker := stages.NewGetItemsResultWorker(
		repo,
		getItemsResCh,
		downstreamReqCh,
		requestBuilders,
	)

	downstreamResultWorker := stages.NewDownstreamResultWorker(
		repo,
		downstreamResCh,
	)

	group, groupCtx := errgroup.WithContext(ctx)

	// Диспетчер закрывает индивидуальные входные каналы GetItems-воркеров.
	group.Go(func() error {
		requests.DispatchRequests(
			groupCtx,
			getItemsReqCh,
			firstReqWorkerCh,
			secondReqWorkerCh,
		)
		return nil
	})

	// Координатор закрывает getItemsResCh после завершения обоих отправителей.
	group.Go(func() error {
		defer getItemsCoordinator.Done()

		firstGetItemsWorker.Run(groupCtx)
		return nil
	})

	group.Go(func() error {
		defer getItemsCoordinator.Done()

		secondGetItemsWorker.Run(groupCtx)
		return nil
	})

	// Обработчик закрывает downstreamReqCh после завершения отправки.
	group.Go(func() error {
		getItemsResultWorker.Run(groupCtx)
		return nil
	})

	// Координатор закрывает downstreamResCh после завершения обоих отправителей.
	group.Go(func() error {
		defer downstreamCoordinator.Done()

		firstDownstreamWorker.Run(groupCtx)
		return nil
	})

	group.Go(func() error {
		defer downstreamCoordinator.Done()

		secondDownstreamWorker.Run(groupCtx)
		return nil
	})

	group.Go(func() error {
		downstreamResultWorker.Run(groupCtx)
		return nil
	})

	// Runner закрывает getItemsReqCh, ожидает всю группу и финализирует пайплайн.
	runner := pipeline.NewRunner(
		repo,
		getItemsReqCh,
		group,
		cancel,
	)

	return runner.Run(ctx, groupCtx, urls)
}
