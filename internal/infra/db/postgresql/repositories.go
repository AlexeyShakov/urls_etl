package postgresql

import (
	"context"

	"github.com/uptrace/bun"

	"urls_etl/internal/domain"
)

type PipelineRepo struct {
	db *bun.DB
}

func NewRepo(db *bun.DB) *PipelineRepo {
	return &PipelineRepo{
		db: db,
	}
}

func (p *PipelineRepo) SavePipeline(
	ctx context.Context,
	pipeline domain.Pipeline,
) (int64, error) {
	model := pipelineModel{
		Status:     string(pipeline.Status),
		FinishedAt: pipeline.FinishedAt,
	}

	_, err := p.db.NewInsert().
		Model(&model).
		Returning("id").
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
		Status:     string(task.Status),
	}

	_, err := p.db.NewInsert().
		Model(&model).
		Returning("id").
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
		Stage:   string(result.Stage),
		Status:  string(result.Status),
		Attempt: result.Attempt,
		Details: result.Details,
	}

	_, err := p.db.NewInsert().
		Model(&model).
		Exec(ctx)

	return mapPostgresError(err)
}

// UpdatePipelineTaskStatuses определяет итоговый статус задач пайплайна.
//
// Задача считается успешной, если все обязательные стадии
// (get_items, fill_items, score_items и log_items)
// завершились со статусом success.
// Во всех остальных случаях задача помечается как failed.
//
// TODO: При добавлении повторных запусков стадий учитывать
// только последнюю попытку для каждой пары task_id и stage.
// TODO: этот вариант хорош только для MVP. Сейчас мы обрабатываем 1 млн тасок сразу
// нужно сделать так, чтобы обрабатывать таски батчами по мере их обработки. В таком случае можно отдавать
// промежуточный результат пользователю. Надо подумать, как это сделать
func (p *PipelineRepo) UpdatePipelineTaskStatuses(
	ctx context.Context,
	pipelineID int64,
) error {
	requiredStages := []domain.Stage{
		domain.StageGetItems,
		domain.StageFillItems,
		domain.StageScoreItems,
		domain.StageLogItems,
	}

	query := `
		UPDATE pipeline_tasks AS task
		SET status = CASE
			WHEN (
				SELECT COUNT(DISTINCT result.stage)
				FROM pipeline_stage_results AS result
				WHERE result.task_id = task.id
					AND result.stage IN (?)
					AND result.status = ?
			) = ?
			THEN ?
			ELSE ?
		END
		WHERE task.pipeline_id = ?
	`

	_, err := p.db.NewRaw(
		query,
		bun.In(requiredStages),
		string(domain.StageStatusSuccess),
		len(requiredStages),
		string(domain.TaskStatusSuccess),
		string(domain.TaskStatusFailed),
		pipelineID,
	).Exec(ctx)

	return mapPostgresError(err)
}

func (p *PipelineRepo) UpdatePipelineStatus(
	ctx context.Context,
	status domain.PipelineStatus,
	pipelineID int64,
) error {
	_, err := p.db.NewUpdate().
		Model((*pipelineModel)(nil)).
		Set("status = ?", string(status)).
		Where("id = ?", pipelineID).
		Exec(ctx)

	return mapPostgresError(err)
}
