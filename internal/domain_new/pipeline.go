package domain_new

import "time"

type PipelineStatus string

const (
	PipelineStatusPending    PipelineStatus = "pending"
	PipelineStatusProcessing PipelineStatus = "processing"
	PipelineStatusFinished   PipelineStatus = "finished"
	PipelineStatusStopped    PipelineStatus = "stopped"
)

type Pipeline struct {
	ID         int64
	Status     PipelineStatus
	CreatedAt  time.Time
	FinishedAt *time.Time
	UpdatedAt  time.Time
}
