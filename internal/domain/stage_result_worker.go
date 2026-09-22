package domain

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"
)

type StageResultWorker struct {
	repo                PipelineRepository
	resultCh            <-chan RequestResult
	requestCh           chan<- PipelineData
	builders            []ItemsRequestBuilder
	pipelineCoordinator IPipelineCoordinator
}

// Run запускает обработку результатов выполненных стадий.
//
// Worker читает результаты HTTP-запросов из resultCh,
// сохраняет их в БД и при необходимости создает задачи
// для следующих стадий pipeline.
//
// Worker завершает работу, если:
//   - закрыт resultCh;
//   - отменен переданный context.
func (w *StageResultWorker) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case result, ok := <-w.resultCh:
			if !ok {
				return
			}

			w.handleOne(ctx, result)

			// Текущая задача полностью обработана независимо от результата.
			w.pipelineCoordinator.Done()
		}
	}
}

func (w *StageResultWorker) handleOne(
	ctx context.Context,
	result RequestResult,
) {
	status := defineStageStatus(result.Response)

	if err := w.saveStageResult(ctx, result, status); err != nil {
		slog.Error(
			"failed to save stage result",
			"pipeline_id", result.PipelineData.PipelineID,
			"task_id", result.PipelineData.TaskID,
			"stage", result.PipelineData.Stage,
			"status", status,
			"err", err,
		)
		return
	}

	slog.Info(
		"stage result saved",
		"pipeline_id", result.PipelineData.PipelineID,
		"task_id", result.PipelineData.TaskID,
		"stage", result.PipelineData.Stage,
		"status", status,
	)

	if status != StageStatusSuccess {
		return
	}

	if err := w.routeStageResult(ctx, result); err != nil {
		slog.Error(
			"failed to route stage result",
			"pipeline_id", result.PipelineData.PipelineID,
			"task_id", result.PipelineData.TaskID,
			"stage", result.PipelineData.Stage,
			"err", err,
		)
	}
}

func (w *StageResultWorker) saveStageResult(
	ctx context.Context,
	result RequestResult,
	status StageStatus,
) error {
	stageResult, err := buildStageResult(result, status)
	if err != nil {
		return err
	}

	if err = w.repo.SaveStageResult(ctx, stageResult); err == nil {
		return nil
	}

	if !IsRetryable(err) {
		return err
	}

	if err = waitBeforeRetry(ctx, 300*time.Millisecond); err != nil {
		return err
	}

	return w.repo.SaveStageResult(ctx, stageResult)
}

func (w *StageResultWorker) routeStageResult(
	ctx context.Context,
	result RequestResult,
) error {
	switch result.PipelineData.Stage {
	case StageGetItems:
		response, err := parseGetItemsResponse(result.Response)
		if err != nil {
			return err
		}

		if len(response.ItemIDs) == 0 {
			return nil
		}

		return w.fanOutGetItemsResult(
			ctx,
			result.PipelineData,
			response.ItemIDs,
		)

	case StageFillItems, StageScoreItems, StageLogItems:
		return nil

	default:
		return fmt.Errorf(
			"unsupported stage: %s",
			result.PipelineData.Stage,
		)
	}
}

func (w *StageResultWorker) fanOutGetItemsResult(
	ctx context.Context,
	data PipelineData,
	itemIDs []int64,
) error {
	requests := make([]PipelineData, 0, len(w.builders))

	for _, buildRequest := range w.builders {
		requestData, err := buildRequest(data, itemIDs)
		if err != nil {
			return err
		}

		requests = append(requests, requestData)
	}

	for _, requestData := range requests {
		w.pipelineCoordinator.Add(1)

		select {
		case w.requestCh <- requestData:

		case <-ctx.Done():
			w.pipelineCoordinator.Done()
			return ctx.Err()
		}
	}

	return nil
}

func NewStageResultWorker(
	repo PipelineRepository,
	resultCh <-chan RequestResult,
	requestCh chan<- PipelineData,
	builders []ItemsRequestBuilder,
	pipelineCoordinator IPipelineCoordinator,
) *StageResultWorker {
	return &StageResultWorker{
		repo:                repo,
		resultCh:            resultCh,
		requestCh:           requestCh,
		builders:            builders,
		pipelineCoordinator: pipelineCoordinator,
	}
}

// defineStageStatus определяет статус стадии по результату HTTP-запроса.
func defineStageStatus(response ResponseData) StageStatus {
	if response.Err != nil {
		return StageStatusFail
	}
	return StageStatusSuccess
}

// buildStageResult формирует объект StageResult для сохранения в БД.
func buildStageResult(
	result RequestResult,
	status StageStatus,
) (StageResult, error) {
	pipelineData := result.PipelineData
	response := result.Response

	details := map[string]any{
		"request": map[string]any{
			"url":     pipelineData.Request.URL,
			"headers": pipelineData.Request.Headers,
			"payload": pipelineData.Request.Payload,
		},
		"response": map[string]any{
			"url":         response.URL,
			"status_code": response.StatusCode,
			"body":        response.Body,
		},
	}

	if response.Err != nil {
		details["error"] = response.Err.Error()
	}

	detailsJSON, err := json.Marshal(details)
	if err != nil {
		return StageResult{}, err
	}

	return StageResult{
		TaskID:  pipelineData.TaskID,
		Stage:   pipelineData.Stage,
		Status:  status,
		Attempt: 1,
		Details: detailsJSON,
	}, nil
}

// waitBeforeRetry ожидает перед повторной попыткой или завершает ожидание при отмене контекста.
func waitBeforeRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// parseGetItemsResponse извлекает идентификаторы товаров из ответа GetItems.
func parseGetItemsResponse(response ResponseData) (GetItemsResponse, error) {
	var getItemsResponse GetItemsResponse

	if err := json.Unmarshal(
		[]byte(response.Body),
		&getItemsResponse,
	); err != nil {
		return GetItemsResponse{}, err
	}

	return getItemsResponse, nil
}
