package domain

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"
)

// HandleStageResult сохраняет результаты выполненных стадий и после успешного
// сохранения передает их на маршрутизацию к следующим стадиям.
func HandleStageResult(
	ctx context.Context,
	repo PipelineRepository,
	resultCh <-chan RequestResult,
	requestCh chan<- PipelineData,
	builders []ItemsRequestBuilder,

) {
	for result := range resultCh {
		status := defineStageStatus(result.Response)
		if err := saveStageResult(ctx, repo, result, status); err != nil {
			slog.Error(
				"failed to save stage result",
				"pipeline_id", result.PipelineData.PipelineID,
				"task_id", result.PipelineData.TaskID,
				"stage", result.PipelineData.Stage,
				"status", status,
				"err", err,
			)
			continue
		}
		slog.Info(
			"stage result saved",
			"pipeline_id", result.PipelineData.PipelineID,
			"task_id", result.PipelineData.TaskID,
			"stage", result.PipelineData.Stage,
			"status", status,
		)
		if status != StatusSuccess {
			continue
		}
		if err := routeStageResult(ctx, result, requestCh, builders); err != nil {
			slog.Error(
				"failed to route stage result",
				"pipeline_id", result.PipelineData.PipelineID,
				"task_id", result.PipelineData.TaskID,
				"stage", result.PipelineData.Stage,
				"err", err,
			)
		}
	}
}

// defineStageStatus определяет статус стадии по результату HTTP-запроса.
func defineStageStatus(response ResponseData) Status {
	if response.Err != nil {
		return StatusFailed
	}
	return StatusSuccess
}

// saveStageResult сохраняет результат выполнения стадии с повторной попыткой при временной ошибке.
func saveStageResult(
	ctx context.Context,
	repo PipelineRepository,
	result RequestResult,
	status Status,
) error {
	stageResult, err := buildStageResult(result, status)
	if err != nil {
		return err
	}
	if err = repo.SaveStageResult(ctx, stageResult); err == nil {
		return nil
	}
	if !IsRetryable(err) {
		return err
	}
	if err = waitBeforeRetry(ctx, 300*time.Millisecond); err != nil {
		return err
	}
	return repo.SaveStageResult(ctx, stageResult)
}

// buildStageResult формирует объект StageResult для сохранения в БД.
func buildStageResult(
	result RequestResult,
	status Status,
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

// routeStageResult определяет дальнейшую обработку результата в зависимости
// от завершенной стадии и останавливает маршрут для конечных стадий.
func routeStageResult(
	ctx context.Context,
	result RequestResult,
	requestCh chan<- PipelineData,
	builders []ItemsRequestBuilder,
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

		return fanOutGetItemsResult(
			ctx,
			result.PipelineData,
			response.ItemIDs,
			requestCh,
			builders,
		)
	// Это "конечные" стадии, после них никаких других действий нет
	case StageFillItems, StageScoreItems, StageLogItems:
		return nil

	default:
		return fmt.Errorf(
			"unsupported stage: %s",
			result.PipelineData.Stage,
		)
	}
}

// fanOutGetItemsResult формирует задачи для всех следующих стадий и отправляет
// их в общий канал только после успешного построения каждого запроса.
func fanOutGetItemsResult(
	ctx context.Context,
	data PipelineData,
	itemIDs []int64,
	requestCh chan<- PipelineData,
	builders []ItemsRequestBuilder,
) error {
	requests := make([]PipelineData, 0, len(builders))

	for _, buildRequest := range builders {
		requestData, err := buildRequest(data, itemIDs)
		if err != nil {
			return err
		}

		requests = append(requests, requestData)
	}

	for _, requestData := range requests {
		select {
		case requestCh <- requestData:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}
