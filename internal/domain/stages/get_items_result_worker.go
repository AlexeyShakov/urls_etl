package stages

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"urls_etl/internal/domain/pipeline"
)

// GetItemsResultWorker сохраняет результаты GetItems и отправляет запросы следующих стадий.
type GetItemsResultWorker struct {
	repo            StageResultRepository
	resultCh        <-chan pipeline.RequestResult
	downstreamReqCh chan<- pipeline.PipelineData
	builders        []ItemsRequestBuilder
}

// NewGetItemsResultWorker создает воркер обработки результатов GetItems.
func NewGetItemsResultWorker(
	repo StageResultRepository,
	resultCh <-chan pipeline.RequestResult,
	downstreamReqCh chan<- pipeline.PipelineData,
	builders []ItemsRequestBuilder,
) *GetItemsResultWorker {
	return &GetItemsResultWorker{
		repo:            repo,
		resultCh:        resultCh,
		downstreamReqCh: downstreamReqCh,
		builders:        builders,
	}
}

// Run обрабатывает результаты и закрывает выходной канал при завершении.
//
// Воркер должен быть единственным отправителем в requestCh.
func (w *GetItemsResultWorker) Run(ctx context.Context) {
	defer close(w.downstreamReqCh)

	for {
		select {
		case <-ctx.Done():
			return

		case result, ok := <-w.resultCh:
			if !ok {
				return
			}

			if err := w.handleOne(ctx, result); err != nil {
				if ctx.Err() != nil {
					return
				}

				slog.Error(
					"failed to handle GetItems result",
					"pipeline_id", result.PipelineData.PipelineID,
					"task_id", result.PipelineData.TaskID,
					"stage", result.PipelineData.Stage,
					"err", err,
				)
			}
		}
	}
}

// handleOne сохраняет результат GetItems и формирует следующие запросы.
func (w *GetItemsResultWorker) handleOne(
	ctx context.Context,
	result pipeline.RequestResult,
) error {
	data := result.PipelineData

	if data.Stage != pipeline.StageGetItems {
		return fmt.Errorf(
			"GetItems result worker received unexpected stage: %s",
			data.Stage,
		)
	}

	status := defineStageStatus(result.Response)

	if err := saveStageResult(ctx, w.repo, result, status); err != nil {
		return fmt.Errorf("save GetItems result: %w", err)
	}

	slog.Debug(
		"stage result saved",
		"pipeline_id", data.PipelineID,
		"task_id", data.TaskID,
		"stage", data.Stage,
		"status", status,
	)

	if status != pipeline.StageStatusSuccess {
		return nil
	}

	payload, err := parseGetItemsResponse(result.Response)
	if err != nil {
		return fmt.Errorf("parse GetItems response: %w", err)
	}

	if len(payload.ItemIDs) == 0 {
		return nil
	}

	return w.fanOutGetItemsResult(ctx, data, payload.ItemIDs)
}

// fanOutGetItemsResult формирует и отправляет запросы следующих стадий.
func (w *GetItemsResultWorker) fanOutGetItemsResult(
	ctx context.Context,
	data pipeline.PipelineData,
	itemIDs []int64,
) error {
	requests := make([]pipeline.PipelineData, 0, len(w.builders))

	for _, buildRequest := range w.builders {
		requestData, err := buildRequest(data, itemIDs)
		if err != nil {
			return fmt.Errorf("build downstream request: %w", err)
		}

		requests = append(requests, requestData)
	}

	for _, requestData := range requests {
		select {
		case w.downstreamReqCh <- requestData:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

// parseGetItemsResponse извлекает идентификаторы товаров из ответа GetItems.
func parseGetItemsResponse(
	response pipeline.ResponseData,
) (ItemIDsPayload, error) {
	var payload ItemIDsPayload

	if err := json.Unmarshal([]byte(response.Body), &payload); err != nil {
		return ItemIDsPayload{}, err
	}

	return payload, nil
}
