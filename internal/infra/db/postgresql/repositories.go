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

func NewRepo(client *pgxpool.Pool) *PipelineRepo {
	return &PipelineRepo{client}
}
