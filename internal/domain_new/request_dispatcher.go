package domain_new

import (
	"context"
	"log/slog"
)

// DispatchRequests распределяет входящие запросы между worker-ами.
//
// Функция читает запросы из reqCh до тех пор, пока канал не будет закрыт.
// Каждый запрос отправляется в первый доступный worker channel.
// После закрытия reqCh функция закрывает worker channels,
// так как именно она является их единственным sender-ом.
func DispatchRequests(ctx context.Context, reqCh <-chan PipelineData, firstWorkerCh, secondWorkerCh chan<- PipelineData) {
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
