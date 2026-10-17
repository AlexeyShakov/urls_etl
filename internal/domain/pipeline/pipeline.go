package pipeline

import "time"

// PipelineStatus определяет состояние пайплайна.
type PipelineStatus string

const (
	PipelineStatusPending    PipelineStatus = "pending"
	PipelineStatusProcessing PipelineStatus = "processing"
	PipelineStatusFinished   PipelineStatus = "finished"
	PipelineStatusStopped    PipelineStatus = "stopped"
)

// Pipeline хранит состояние и время выполнения пайплайна.
type Pipeline struct {
	ID         int64
	Status     PipelineStatus
	CreatedAt  time.Time
	FinishedAt *time.Time
	UpdatedAt  time.Time
}
