package stages

import (
	"context"
	"fmt"
	"log/slog"

	"urls_etl/internal/domain/pipeline"
)

// DownstreamResultWorker сохраняет результаты стадий FillItems, ScoreItems и LogItems.
type DownstreamResultWorker struct {
	repo     StageResultRepository
	resultCh <-chan pipeline.RequestResult
}

// NewDownstreamResultWorker создает воркер обработки результатов следующих стадий.
func NewDownstreamResultWorker(
	repo StageResultRepository,
	resultCh <-chan pipeline.RequestResult,
) *DownstreamResultWorker {
	return &DownstreamResultWorker{
		repo:     repo,
		resultCh: resultCh,
	}
}

// Run обрабатывает результаты до закрытия канала или отмены контекста.
func (w *DownstreamResultWorker) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case result, ok := <-w.resultCh:
			if !ok {
				return
			}

			if err := w.handleOne(ctx, result); err != nil {
				if ctx.Err() != nil {
					return
				}

				slog.Error(
					"failed to handle downstream result",
					"pipeline_id", result.PipelineData.PipelineID,
					"task_id", result.PipelineData.TaskID,
					"stage", result.PipelineData.Stage,
					"err", err,
				)
			}
		}
	}
}

// handleOne проверяет стадию и сохраняет результат ее выполнения.
func (w *DownstreamResultWorker) handleOne(
	ctx context.Context,
	result pipeline.RequestResult,
) error {
	data := result.PipelineData

	switch data.Stage {
	case pipeline.StageFillItems,
		pipeline.StageScoreItems,
		pipeline.StageLogItems:
		// Эти стадии завершаются сохранением результата.
	default:
		return fmt.Errorf(
			"downstream result worker received unexpected stage: %s",
			data.Stage,
		)
	}

	status := defineStageStatus(result.Response)

	if err := saveStageResult(ctx, w.repo, result, status); err != nil {
		return fmt.Errorf("save downstream result: %w", err)
	}

	slog.Debug(
		"stage result saved",
		"pipeline_id", data.PipelineID,
		"task_id", data.TaskID,
		"stage", data.Stage,
		"status", status,
	)

	return nil
}
