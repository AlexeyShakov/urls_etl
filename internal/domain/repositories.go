package domain

import "context"

type PipelineRepository interface {
	SavePipeline(ctx context.Context, pipeline Pipeline) (int64, error)
	SavePipelineTask(ctx context.Context, task PipelineTask) (int64, error)
	SaveStageResult(ctx context.Context, result StageResult) error
	UpdatePipelineTaskStatuses(ctx context.Context, pipelineID int64) error
	UpdatePipelineStatus(ctx context.Context, status PipelineStatus, pipelineID int64) error
}
