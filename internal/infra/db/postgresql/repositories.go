package postgresql

import (
	"context"

	"urls_etl/internal/domain"

	"github.com/jackc/pgx/v5/pgxpool"
)

type PipelineRepo struct {
	client *pgxpool.Pool
}

func (p *PipelineRepo) SavePipeline(ctx context.Context, pipeline domain.Pipeline) (int64, error) {
	query := `
        INSERT INTO pipelines (status, finished_at)
        VALUES ($1, $2)
        RETURNING id
    `

	var id int64

	err := p.client.QueryRow(
		ctx,
		query,
		pipeline.Status,
		pipeline.FinishedAt,
	).Scan(&id)

	return id, mapPostgresError(err)
}

func (p *PipelineRepo) SavePipelineTask(ctx context.Context, task domain.PipelineTask) (int64, error) {
	query := `
        INSERT INTO pipeline_tasks (pipeline_id, source_url, details, status)
        VALUES ($1, $2, $3, $4)
        RETURNING id
    `

	var id int64

	err := p.client.QueryRow(
		ctx,
		query,
		task.PipelineID,
		task.SourceURL,
		task.Details,
		task.Status,
	).Scan(&id)

	return id, mapPostgresError(err)
}
func (p *PipelineRepo) SaveStageResult(ctx context.Context, result domain.StageResult) error {
	query := `
        INSERT INTO pipeline_stage_results (
            task_id,
            stage,
            status,
            attempt,
            details
        )
        VALUES ($1,$2,$3,$4,$5)
    `

	_, err := p.client.Exec(
		ctx,
		query,
		result.TaskID,
		result.Stage,
		result.Status,
		result.Attempt,
		result.Details,
	)

	return mapPostgresError(err)
}

// UpdatePipelineTaskStatuses определяет итоговый статус задач пайплайна.
//
// Задача считается успешной, если все обязательные стадии
// (get_items, fill_items, score_items и log_items)
// завершились со статусом StatusSuccess.
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
	query := `
		UPDATE pipeline_tasks AS task
		SET status = CASE
			WHEN (
				SELECT COUNT(DISTINCT result.stage)
				FROM pipeline_stage_results AS result
				WHERE result.task_id = task.id
				  AND result.stage = ANY($2)
				  AND result.status = $3
			) = $4
			THEN $5
			ELSE $6
		END
		WHERE task.pipeline_id = $1
	`

	requiredStages := []domain.Stage{
		domain.StageGetItems,
		domain.StageFillItems,
		domain.StageScoreItems,
		domain.StageLogItems,
	}

	_, err := p.client.Exec(
		ctx,
		query,
		pipelineID,
		requiredStages,
		domain.StageStatusSuccess,
		len(requiredStages),
		domain.TaskStatusSuccess,
		domain.TaskStatusFailed,
	)

	return mapPostgresError(err)
}

func (p *PipelineRepo) UpdatePipelineStatus(
	ctx context.Context,
	status domain.PipelineStatus,
	pipelineID int64,
) error {
	query := `
		UPDATE pipelines 
		SET status = $1
		WHERE pipeline_tasks.id = $2
	`
	_, err := p.client.Exec(
		ctx,
		query,
		status,
		pipelineID,
	)

	return mapPostgresError(err)

}

func NewRepo(client *pgxpool.Pool) *PipelineRepo {
	return &PipelineRepo{client}
}
