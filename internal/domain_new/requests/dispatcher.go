package requests

import (
	"context"
	"log/slog"

	"urls_etl/internal/domain_new/pipeline"
)

// DispatchRequests распределяет запросы между двумя каналами воркеров.
//
// Завершается при закрытии reqCh или отмене контекста.
// Закрывает оба выходных канала, для которых является единственным отправителем.
func DispatchRequests(
	ctx context.Context,
	reqCh <-chan pipeline.PipelineData,
	firstWorkerCh chan<- pipeline.PipelineData,
	secondWorkerCh chan<- pipeline.PipelineData,
) {
	defer func() {
		close(firstWorkerCh)
		close(secondWorkerCh)
	}()

	for {
		select {
		case <-ctx.Done():
			return

		case req, ok := <-reqCh:
			if !ok {
				return
			}

			select {
			case firstWorkerCh <- req:
				slog.Debug(
					"request dispatched",
					"worker_id", 1,
					"pipeline_id", req.PipelineID,
					"task_id", req.TaskID,
				)

			case secondWorkerCh <- req:
				slog.Debug(
					"request dispatched",
					"worker_id", 2,
					"pipeline_id", req.PipelineID,
					"task_id", req.TaskID,
				)

			case <-ctx.Done():
				return
			}
		}
	}
}
