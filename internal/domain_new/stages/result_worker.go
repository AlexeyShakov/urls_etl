package stages

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"urls_etl/internal/domain_new/pipeline"
)

// StageResultRepository сохраняет результаты выполнения стадий.
type StageResultRepository interface {
	SaveStageResult(ctx context.Context, result pipeline.StageResult) error
}

// StageResultCoordinator отслеживает незавершенные работы пайплайна.
type StageResultCoordinator interface {
	Add(n int)
	Done()
}

// StageResultWorker сохраняет результаты и формирует запросы следующих стадий.
type StageResultWorker struct {
	repo                StageResultRepository
	resultCh            <-chan pipeline.RequestResult
	requestCh           chan<- pipeline.PipelineData
	builders            []ItemsRequestBuilder
	pipelineCoordinator StageResultCoordinator
}

// NewStageResultWorker создает воркер обработки результатов стадий.
func NewStageResultWorker(
	repo StageResultRepository,
	resultCh <-chan pipeline.RequestResult,
	requestCh chan<- pipeline.PipelineData,
	builders []ItemsRequestBuilder,
	pipelineCoordinator StageResultCoordinator,
) *StageResultWorker {
	return &StageResultWorker{
		repo:                repo,
		resultCh:            resultCh,
		requestCh:           requestCh,
		builders:            builders,
		pipelineCoordinator: pipelineCoordinator,
	}
}

// Run обрабатывает результаты до закрытия канала или отмены контекста.
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
			w.pipelineCoordinator.Done()
		}
	}
}

// handleOne сохраняет результат и запускает обработку следующих стадий.
func (w *StageResultWorker) handleOne(
	ctx context.Context,
	result pipeline.RequestResult,
) {
	status := defineStageStatus(result.Response)
	data := result.PipelineData

	if err := w.saveStageResult(ctx, result, status); err != nil {
		slog.Error(
			"failed to save stages result",
			"pipeline_id", data.PipelineID,
			"task_id", data.TaskID,
			"stages", data.Stage,
			"status", status,
			"err", err,
		)
		return
	}

	slog.Info(
		"stages result saved",
		"pipeline_id", data.PipelineID,
		"task_id", data.TaskID,
		"stages", data.Stage,
		"status", status,
	)

	if status != pipeline.StageStatusSuccess {
		return
	}

	if err := w.routeStageResult(ctx, result); err != nil {
		slog.Error(
			"failed to route stages result",
			"pipeline_id", data.PipelineID,
			"task_id", data.TaskID,
			"stages", data.Stage,
			"err", err,
		)
	}
}

// saveStageResult сохраняет результат с одной повторной попыткой при временной ошибке.
func (w *StageResultWorker) saveStageResult(
	ctx context.Context,
	result pipeline.RequestResult,
	status pipeline.StageStatus,
) error {
	stageResult, err := buildStageResult(result, status)
	if err != nil {
		return err
	}

	err = w.repo.SaveStageResult(ctx, stageResult)
	if err == nil || !pipeline.IsRetryable(err) {
		return err
	}

	if err := waitBeforeRetry(ctx, 300*time.Millisecond); err != nil {
		return err
	}

	return w.repo.SaveStageResult(ctx, stageResult)
}

// routeStageResult определяет дальнейшую обработку успешной стадии.
func (w *StageResultWorker) routeStageResult(
	ctx context.Context,
	result pipeline.RequestResult,
) error {
	switch result.PipelineData.Stage {
	case pipeline.StageGetItems:
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

	case pipeline.StageFillItems,
		pipeline.StageScoreItems,
		pipeline.StageLogItems:
		return nil

	default:
		return fmt.Errorf(
			"unsupported stages: %s",
			result.PipelineData.Stage,
		)
	}
}

// fanOutGetItemsResult формирует и отправляет запросы следующих стадий.
func (w *StageResultWorker) fanOutGetItemsResult(
	ctx context.Context,
	data pipeline.PipelineData,
	itemIDs []int64,
) error {
	requests := make([]pipeline.PipelineData, 0, len(w.builders))

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

// defineStageStatus определяет статус стадии по наличию ошибки запроса.
func defineStageStatus(response pipeline.ResponseData) pipeline.StageStatus {
	if response.Err != nil {
		return pipeline.StageStatusFail
	}

	return pipeline.StageStatusSuccess
}

// buildStageResult формирует результат стадии с подробностями запроса и ответа.
func buildStageResult(
	result pipeline.RequestResult,
	status pipeline.StageStatus,
) (pipeline.StageResult, error) {
	data := result.PipelineData
	response := result.Response

	details := map[string]any{
		"request": map[string]any{
			"url":     data.Request.URL,
			"headers": data.Request.Headers,
			"payload": data.Request.Payload,
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
		return pipeline.StageResult{}, err
	}

	return pipeline.StageResult{
		TaskID:  data.TaskID,
		Stage:   data.Stage,
		Status:  status,
		Attempt: 1,
		Details: detailsJSON,
	}, nil
}

// waitBeforeRetry ожидает задержку или отмену контекста.
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
func parseGetItemsResponse(
	response pipeline.ResponseData,
) (ItemIDsPayload, error) {
	var payload ItemIDsPayload

	if err := json.Unmarshal([]byte(response.Body), &payload); err != nil {
		return ItemIDsPayload{}, err
	}

	return payload, nil
}
