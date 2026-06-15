package domain

import (
	"context"
	"encoding/json"
	"log/slog"
)

func RunPipeline(ctx context.Context, urls []RequestData, ch chan<- PipelineData, repo PipelineRepository) {
	defer close(ch)

	pipelineID, err := savePipeline(ctx, repo)
	if err != nil {
		slog.Error(SavingPipeFail, "err", err)

		if IsRetryable(err) {
			pipelineID, err = savePipeline(ctx, repo)
			if err != nil {
				slog.Error(RepeatingSavingPipeFail, "err", err)
				return
			}
		} else {
			slog.Error(DBUnavailableNonRetryableSavingPipe, "err", err)
			return
		}
	}

	for _, req := range urls {
		//todo можно вынести сохранение таски в воркер, тогда мы сможем распарралелить это действие. Сейчас сохранение
		//тасок происходит синхронно
		taskID, err := saveTask(ctx, repo, req, pipelineID)
		if err != nil {
			slog.Error(SavingTaskFail, "pipeline_id", pipelineID, "url", req.URL, "err", err)

			if IsRetryable(err) {
				taskID, err = saveTask(ctx, repo, req, pipelineID)
				if err != nil {
					slog.Error(RepeatingSavingTaskFail, "pipeline_id", pipelineID, "url", req.URL, "err", err)
					continue
				}
			} else {
				slog.Error(DBUnavailableNonRetryableSavingTask, "pipeline_id", pipelineID, "url", req.URL, "err", err)
				continue
			}
		}

		ch <- PipelineData{
			PipelineID: pipelineID,
			TaskID:     taskID,
			Request:    req,
		}
	}
}

func savePipeline(ctx context.Context, repo PipelineRepository) (int64, error) {
	pipelineID, err := repo.SavePipeline(ctx, Pipeline{
		Status:     StatusProcessing,
		FinishedAt: nil,
	})
	return pipelineID, err
}

func saveTask(
	ctx context.Context,
	repo PipelineRepository,
	request RequestData,
	pipelineID int64,
) (int64, error) {

	detailsJSON, err := json.Marshal(request)
	if err != nil {
		return 0, err
	}

	taskID, err := repo.SavePipelineTask(
		ctx,
		PipelineTask{
			PipelineID: pipelineID,
			SourceURL:  request.URL,
			Details:    detailsJSON,
			Status:     StatusProcessing,
		},
	)

	return taskID, err
}
