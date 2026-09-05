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

type RequestWorker struct {
	id        int
	reqCh     <-chan PipelineData
	requester Requester
	resultCh  chan<- RequestResult
}

// Run запускает worker для обработки запросов.
//
// Для каждой входящей задачи worker:
//   - выполняет запрос к стороннему сервису;
//   - передает результат запроса дальше в канал обработки результатов.
//
// Worker завершает работу, если:
//   - закрыт входной канал;
//   - отменен переданный context.
func (w *RequestWorker) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case pipelineData, ok := <-w.reqCh:
			if !ok {
				return
			}

			slog.Info(
				"request worker started",
				"worker_id", w.id,
				"pipeline_id", pipelineData.PipelineID,
				"task_id", pipelineData.TaskID,
				"url", pipelineData.Request.URL,
				"stage", pipelineData.Stage,
			)

			resp := w.requester.Do(ctx, pipelineData)

			result := RequestResult{
				PipelineData: pipelineData,
				Response:     resp,
			}

			select {
			case w.resultCh <- result:
			case <-ctx.Done():
				return
			}
		}
	}
}

func NewRequestWorker(
	id int,
	reqCh <-chan PipelineData,
	requester Requester,
	resultCh chan<- RequestResult,
) *RequestWorker {
	return &RequestWorker{
		id:        id,
		reqCh:     reqCh,
		requester: requester,
		resultCh:  resultCh,
	}
}
