package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
)

type RunPipelineRepository interface {
	SavePipeline(ctx context.Context, pipeline Pipeline) (int64, error)
	SavePipelineTask(ctx context.Context, task PipelineTask) (int64, error)
	UpdatePipelineTaskStatuses(ctx context.Context, pipelineID int64) error
	UpdatePipelineStatus(
		ctx context.Context,
		status PipelineStatus,
		pipelineID int64,
	) error
}

type RunPipelineCoordinator interface {
	// Add увеличивает количество незавершенных работ.
	Add(n int)

	// Done отмечает завершение одной работы.
	Done()

	// FinishInitialUrlsSubmission отмечает завершение отправки начальных задач.
	FinishInitialUrlsSubmission()

	// Wait ожидает завершения всех работ или отмены контекста.
	Wait(ctx context.Context) error
}

// RunPipeline создает задачи, отправляет начальные запросы и финализирует статусы.
//
// После завершения всех работ закрывает ch.
// При досрочной ошибке не закрывает ch: вызывающий код должен отменить
// общий контекст и дождаться завершения всех воркеров.
func RunPipeline(
	ctx context.Context,
	urls []RequestData,
	ch chan<- PipelineData,
	repo RunPipelineRepository,
	coordinator RunPipelineCoordinator,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	pipelineID, err := savePipeline(ctx, repo)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}

		if !IsRetryable(err) {
			return fmt.Errorf("create pipeline: %w", err)
		}

		slog.Warn(SavingPipeFail, "err", err)

		pipelineID, err = savePipeline(ctx, repo)
		if err != nil {
			return fmt.Errorf("create pipeline after retry: %w", err)
		}
	}

	for _, req := range urls {
		if err := ctx.Err(); err != nil {
			return err
		}

		taskID, err := saveTask(ctx, repo, req, pipelineID)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}

			if !IsRetryable(err) {
				slog.Error(
					SavingTaskFail,
					"pipeline_id", pipelineID,
					"url", req.URL,
					"err", err,
				)
				continue
			}

			slog.Warn(
				SavingTaskFail,
				"pipeline_id", pipelineID,
				"url", req.URL,
				"err", err,
			)

			taskID, err = saveTask(ctx, repo, req, pipelineID)
			if err != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return ctxErr
				}

				slog.Error(
					RepeatingSavingTaskFail,
					"pipeline_id", pipelineID,
					"url", req.URL,
					"err", err,
				)
				continue
			}
		}

		coordinator.Add(1)

		select {
		case ch <- PipelineData{
			PipelineID: pipelineID,
			TaskID:     taskID,
			Request:    req,
			Stage:      StageGetItems,
		}:
		case <-ctx.Done():
			coordinator.Done()
			return ctx.Err()
		}
	}

	coordinator.FinishInitialUrlsSubmission()

	if err := coordinator.Wait(ctx); err != nil {
		return err
	}

	// Все работы завершены, включая отправку запросов следующих стадий.
	close(ch)

	if err := repo.UpdatePipelineTaskStatuses(ctx, pipelineID); err != nil {
		return fmt.Errorf(
			"update task statuses for pipeline %d: %w",
			pipelineID,
			err,
		)
	}

	if err := repo.UpdatePipelineStatus(
		ctx,
		PipelineStatusFinished,
		pipelineID,
	); err != nil {
		return fmt.Errorf(
			"finish pipeline %d: %w",
			pipelineID,
			err,
		)
	}

	return nil
}

// savePipeline сохраняет пайплайн в состоянии обработки.
func savePipeline(
	ctx context.Context,
	repo RunPipelineRepository,
) (int64, error) {
	return repo.SavePipeline(ctx, Pipeline{
		Status: PipelineStatusProcessing,
	})
}

// saveTask сохраняет задачу с подробностями исходного запроса.
func saveTask(
	ctx context.Context,
	repo RunPipelineRepository,
	request RequestData,
	pipelineID int64,
) (int64, error) {
	detailsJSON, err := json.Marshal(request)
	if err != nil {
		return 0, fmt.Errorf("marshal task request: %w", err)
	}

	return repo.SavePipelineTask(ctx, PipelineTask{
		PipelineID: pipelineID,
		SourceURL:  request.URL,
		Details:    detailsJSON,
		Status:     TaskStatusProcessing,
	})
}
