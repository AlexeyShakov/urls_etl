package stages

import (
	"context"
	"encoding/json"
	"time"

	"urls_etl/internal/domain/pipeline"
)

// StageResultRepository сохраняет результаты выполнения стадий.
type StageResultRepository interface {
	SaveStageResult(ctx context.Context, result pipeline.StageResult) error
}

// saveStageResult сохраняет результат с одним ретраем при временной ошибке.
func saveStageResult(
	ctx context.Context,
	repo StageResultRepository,
	result pipeline.RequestResult,
	status pipeline.StageStatus,
) error {
	stageResult, err := buildStageResult(result, status)
	if err != nil {
		return err
	}

	err = repo.SaveStageResult(ctx, stageResult)
	if err == nil || !pipeline.IsRetryable(err) {
		return err
	}

	if err := waitBeforeRetry(ctx, 300*time.Millisecond); err != nil {
		return err
	}

	return repo.SaveStageResult(ctx, stageResult)
}

// defineStageStatus определяет статус стадии по наличию ошибки запроса.
func defineStageStatus(response pipeline.ResponseData) pipeline.StageStatus {
	if response.Err != nil {
		return pipeline.StageStatusFail
	}

	return pipeline.StageStatusSuccess
}

// buildStageResult формирует запись с подробностями запроса и ответа.
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
