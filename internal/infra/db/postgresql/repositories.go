package postgresql

import (
	"context"

	"urls_etl/internal/domain"

	"github.com/uptrace/bun"
)

type PipelineRepo struct {
	db *bun.DB
}

func (p *PipelineRepo) SavePipeline(
	ctx context.Context,
	pipeline domain.Pipeline,
) (int64, error) {
	model := pipelineModel{
		Status:     pipeline.Status,
		FinishedAt: pipeline.FinishedAt,
	}

	_, err := p.db.NewInsert().
		Model(&model).
		Exec(ctx)

	if err != nil {
		return 0, mapPostgresError(err)
	}

	return model.ID, nil
}

func (p *PipelineRepo) SavePipelineTask(
	ctx context.Context,
	task domain.PipelineTask,
) (int64, error) {
	model := pipelineTaskModel{
		PipelineID: task.PipelineID,
		SourceURL:  task.SourceURL,
		Details:    task.Details,
		Status:     task.Status,
	}

	_, err := p.db.NewInsert().
		Model(&model).
		Exec(ctx)

	if err != nil {
		return 0, mapPostgresError(err)
	}

	return model.ID, nil
}

func (p *PipelineRepo) SaveStageResult(
	ctx context.Context,
	result domain.StageResult,
) error {
	model := stageResultModel{
		TaskID:  result.TaskID,
		Stage:   result.Stage,
		Status:  result.Status,
		Attempt: result.Attempt,
		Details: result.Details,
	}

	_, err := p.db.NewInsert().
		Model(&model).
		Exec(ctx)

	return mapPostgresError(err)
}

func NewRepo(db *bun.DB) *PipelineRepo {
	return &PipelineRepo{db: db}
}
