package domain

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"
)

func HandleStageResult(
	ctx context.Context,
	repo PipelineRepository,
	resultCh <-chan RequestResult,
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
		// TODO: route successfully persisted results according to their stage.
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
