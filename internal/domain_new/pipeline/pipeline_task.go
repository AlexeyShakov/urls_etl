package pipeline

import (
	"encoding/json"
	"time"
)

// TaskStatus определяет состояние задачи пайплайна.
type TaskStatus string

const (
	TaskStatusProcessing TaskStatus = "processing"
	TaskStatusSuccess    TaskStatus = "success"
	TaskStatusFailed     TaskStatus = "failed"
)

// PipelineTask хранит исходный запрос и состояние задачи пайплайна.
type PipelineTask struct {
	ID         int64
	PipelineID int64
	SourceURL  string
	Details    json.RawMessage
	Status     TaskStatus
	CreatedAt  time.Time
	UpdatedAt  time.Time
}
