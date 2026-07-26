package domain

import (
	"context"
	"log/slog"
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
//   - передает результат запроса дальше в канал для обработки результатов
//
// Worker завершает работу после закрытия входного канала.
func RequestWorker(
	ctx context.Context,
	workerID int,
	reqCh <-chan PipelineData,
	requester Requester,
	resultCh chan<- RequestResult,
) {
	for pipelineData := range reqCh {
		slog.Info(
			"request worker started",
			"worker_id", workerID,
			"pipeline_id", pipelineData.PipelineID,
			"task_id", pipelineData.TaskID,
			"url", pipelineData.Request.URL,
			"stage", pipelineData.Stage,
		)
		resp := requester.Do(ctx, pipelineData)
		resultCh <- RequestResult{
			PipelineData: pipelineData,
			Response:     resp,
		}
	}
}
