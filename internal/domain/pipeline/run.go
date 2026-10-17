package pipeline

import (
	"context"
	"encoding/json"
	"errors"
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

// ExecutionWaiter ожидает завершения всех воркеров и обработчиков результатов.
type ExecutionWaiter interface {
	Wait() error
}

// Runner управляет созданием, выполнением и финализацией пайплайна.
//
// Используется для одного запуска и является единственным отправителем в ch.
type Runner struct {
	repo   RunPipelineRepository
	ch     chan<- PipelineData
	waiter ExecutionWaiter
	cancel context.CancelFunc
}

// NewRunner создает оркестратор пайплайна.
func NewRunner(
	repo RunPipelineRepository,
	ch chan<- PipelineData,
	waiter ExecutionWaiter,
	cancel context.CancelFunc,
) *Runner {
	return &Runner{
		repo:   repo,
		ch:     ch,
		waiter: waiter,
		cancel: cancel,
	}
}

// Run создает пайплайн, отправляет задачи и финализирует результаты обработки.
func (p *Runner) Run(
	ctx context.Context,
	executionCtx context.Context,
	urls []RequestData,
) error {
	pipelineID, err := p.createPipeline(executionCtx)
	if err != nil {
		close(p.ch)
		p.cancel()

		return errors.Join(err, p.waiter.Wait())
	}

	processErr := p.processURLs(executionCtx, urls, pipelineID)

	// Начальные запросы больше не появятся.
	close(p.ch)

	if processErr != nil {
		p.cancel()
	}

	waitErr := p.waiter.Wait()
	if err := errors.Join(processErr, waitErr); err != nil {
		return err
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	return p.finalizePipeline(ctx, pipelineID)
}

// createPipeline сохраняет пайплайн с одним ретраем при временной ошибке.
func (p *Runner) createPipeline(ctx context.Context) (int64, error) {
	pipelineID, err := p.savePipeline(ctx)
	if err == nil {
		return pipelineID, nil
	}

	if ctxErr := ctx.Err(); ctxErr != nil {
		return 0, ctxErr
	}

	if !IsRetryable(err) {
		return 0, fmt.Errorf("create pipeline: %w", err)
	}

	slog.Warn(SavingPipeFail, "err", err)

	pipelineID, err = p.savePipeline(ctx)
	if err != nil {
		return 0, fmt.Errorf("create pipeline after retry: %w", err)
	}

	return pipelineID, nil
}

// processURLs сохраняет задачи и отправляет начальные запросы GetItems.
func (p *Runner) processURLs(
	ctx context.Context,
	urls []RequestData,
	pipelineID int64,
) error {
	for _, req := range urls {
		if err := ctx.Err(); err != nil {
			return err
		}

		taskID, err := p.saveTask(ctx, req, pipelineID)
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

			taskID, err = p.saveTask(ctx, req, pipelineID)
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

		select {
		case p.ch <- PipelineData{
			PipelineID: pipelineID,
			TaskID:     taskID,
			Request:    req,
			Stage:      StageGetItems,
		}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

// finalizePipeline обновляет статусы задач и отмечает завершение пайплайна.
func (p *Runner) finalizePipeline(
	ctx context.Context,
	pipelineID int64,
) error {
	if err := p.repo.UpdatePipelineTaskStatuses(ctx, pipelineID); err != nil {
		return fmt.Errorf(
			"update task statuses for pipeline %d: %w",
			pipelineID,
			err,
		)
	}

	if err := p.repo.UpdatePipelineStatus(
		ctx,
		PipelineStatusFinished,
		pipelineID,
	); err != nil {
		return fmt.Errorf("finish pipeline %d: %w", pipelineID, err)
	}

	return nil
}

// savePipeline сохраняет пайплайн в состоянии обработки.
func (p *Runner) savePipeline(ctx context.Context) (int64, error) {
	return p.repo.SavePipeline(ctx, Pipeline{
		Status: PipelineStatusProcessing,
	})
}

// saveTask сохраняет задачу с подробностями исходного запроса.
func (p *Runner) saveTask(
	ctx context.Context,
	request RequestData,
	pipelineID int64,
) (int64, error) {
	detailsJSON, err := json.Marshal(request)
	if err != nil {
		return 0, fmt.Errorf("marshal task request: %w", err)
	}

	return p.repo.SavePipelineTask(ctx, PipelineTask{
		PipelineID: pipelineID,
		SourceURL:  request.URL,
		Details:    detailsJSON,
		Status:     TaskStatusProcessing,
	})
}
