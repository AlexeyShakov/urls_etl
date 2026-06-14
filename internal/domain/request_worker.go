package domain

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"
)

type Requester interface {
	Do(
		ctx context.Context,
		pipelineData PipelineData,
	) ResponseData
}

// RequestWorker обрабатывает задачи отправления запросов на сторонний сервис
//
// Для каждой входящей задачи worker:
//   - выполняет запрос к стороннему сервису;
//   - сохраняет результат выполнения этапа в БД;
//   - логирует успешное или ошибочное выполнение.
//
// Worker завершает работу после закрытия входного канала.
//
// В дальнейшем здесь появится передача результата в выходной канал для обработки следующий стадий
func RequestWorker(
	ctx context.Context,
	workerID int,
	reqCh <-chan PipelineData,
	requester Requester,
	repo PipelineRepository,
) {
	for pipelineData := range reqCh {
		slog.Info(
			"request worker started",
			"worker_id", workerID,
			"pipeline_id", pipelineData.PipelineID,
			"task_id", pipelineData.TaskID,
			"url", pipelineData.Request.URL,
		)

		resp := requester.Do(ctx, pipelineData)

		if resp.Err != nil {
			slog.Error(
				"request worker failed",
				"worker_id", workerID,
				"pipeline_id", pipelineData.PipelineID,
				"task_id", pipelineData.TaskID,
				"url", pipelineData.Request.URL,
				"status_code", resp.StatusCode,
				"err", resp.Err,
			)

			if err := saveToDB(ctx, repo, pipelineData, resp, StatusFailed); err != nil {
				slog.Error(
					"failed to save get_items stage result",
					"worker_id", workerID,
					"pipeline_id", pipelineData.PipelineID,
					"task_id", pipelineData.TaskID,
					"url", pipelineData.Request.URL,
					"err", err,
				)
			}

			continue
		}

		slog.Info(
			"request worker finished",
			"worker_id", workerID,
			"pipeline_id", pipelineData.PipelineID,
			"task_id", pipelineData.TaskID,
			"url", resp.URL,
			"status_code", resp.StatusCode,
		)

		if err := saveToDB(ctx, repo, pipelineData, resp, StatusSuccess); err != nil {
			slog.Error(
				"failed to save get_items stage result",
				"worker_id", workerID,
				"pipeline_id", pipelineData.PipelineID,
				"task_id", pipelineData.TaskID,
				"url", pipelineData.Request.URL,
				"err", err,
			)
		}

		// TODO: дальше будем парсить resp.Body и передавать item_ids в следующий stage.
	}
}

func saveToDB(
	ctx context.Context,
	repo PipelineRepository,
	pipelineData PipelineData,
	resp ResponseData,
	status string,
) error {
	details := map[string]any{
		"request": map[string]any{
			"url":     pipelineData.Request.URL,
			"headers": pipelineData.Request.Headers,
			"payload": pipelineData.Request.Payload,
		},
		"response": map[string]any{
			"url":         resp.URL,
			"status_code": resp.StatusCode,
			"body":        resp.Body,
		},
	}

	if resp.Err != nil {
		details["error"] = resp.Err.Error()
	}

	detailsJSON, err := json.Marshal(details)
	if err != nil {
		return err
	}

	stageResult := StageResult{
		TaskID:  pipelineData.TaskID,
		Stage:   StageGetItems, // todo в дальнейшем предусмотреть динамическое подставление stage
		Status:  status,
		Attempt: 1,
		Details: detailsJSON,
	}

	err = repo.SaveStageResult(ctx, stageResult)
	if err == nil {
		return nil
	}

	if !IsRetryable(err) {
		return err
	}

	time.Sleep(300 * time.Millisecond)

	err = repo.SaveStageResult(ctx, stageResult)
	if err != nil {
		return err
	}

	return nil
}
