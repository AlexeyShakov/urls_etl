package requests

import (
	"context"
	"log/slog"

	"urls_etl/internal/domain_new/pipeline"
)

// Requester выполняет запрос стадии пайплайна.
type Requester interface {
	Do(
		ctx context.Context,
		data pipeline.PipelineData,
	) pipeline.ResponseData
}

// RequestWorker выполняет запросы и передает результаты обработки.
type RequestWorker struct {
	id        int
	reqCh     <-chan pipeline.PipelineData
	requester Requester
	resultCh  chan<- pipeline.RequestResult
}

// NewRequestWorker создает воркер выполнения запросов.
func NewRequestWorker(
	id int,
	reqCh <-chan pipeline.PipelineData,
	requester Requester,
	resultCh chan<- pipeline.RequestResult,
) *RequestWorker {
	return &RequestWorker{
		id:        id,
		reqCh:     reqCh,
		requester: requester,
		resultCh:  resultCh,
	}
}

// Run обрабатывает запросы до закрытия входного канала или отмены контекста.
func (w *RequestWorker) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case data, ok := <-w.reqCh:
			if !ok {
				return
			}

			slog.Debug(
				"request processing started",
				"worker_id", w.id,
				"pipeline_id", data.PipelineID,
				"task_id", data.TaskID,
				"url", data.Request.URL,
				"stage", data.Stage,
			)

			response := w.requester.Do(ctx, data)

			result := pipeline.RequestResult{
				PipelineData: data,
				Response:     response,
			}

			select {
			case w.resultCh <- result:
			case <-ctx.Done():
				return
			}
		}
	}
}
