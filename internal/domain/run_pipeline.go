package domain

import (
	"context"
	"encoding/json"
	"log/slog"
)

// RunPipeline:
// 1. создает Pipeline и PipelineTask;
// 2. отправляет начальные GetItems задачи;
// 3. ждет завершения всех созданных задач через coordinator;
// 4. закрывает requestChannel;
// 5. финализирует статусы.
func RunPipeline(
	ctx context.Context,
	urls []RequestData,
	ch chan<- PipelineData,
	repo PipelineRepository,
	pipelineCoordinator IPipelineCoordinator,
) {
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
		pipelineCoordinator.Add(1)
		ch <- PipelineData{
			PipelineID: pipelineID,
			TaskID:     taskID,
			Request:    req,
			Stage:      StageGetItems,
		}
	}
	pipelineCoordinator.FinishInitialUrlsSubmission()
	// Ждем завершения всех задач пайплайна.
	pipelineCoordinator.Wait()
	// Новых запросов больше не будет, поэтому канал можно безопасно закрыть.
	close(ch)
	if err := repo.UpdatePipelineTaskStatuses(ctx, pipelineID); err != nil {
		slog.Error(
			"failed to update pipeline task statuses",
			"pipeline_id", pipelineID,
			"err", err,
		)
		return
	}
	if err := repo.UpdatePipelineStatus(ctx, PipelineStatusFinished, pipelineID); err != nil {
		slog.Error(
			"failed to update pipeline task statuses",
			"pipeline_id", pipelineID,
			"err", err,
		)
		return
	}

}

func savePipeline(ctx context.Context, repo PipelineRepository) (int64, error) {
	pipelineID, err := repo.SavePipeline(ctx, Pipeline{
		Status:     PipelineStatusProcessing,
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
			Status:     TaskStatusProcessing,
		},
	)

	return taskID, err
}
