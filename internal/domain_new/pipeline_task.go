package domain_new

import (
	"encoding/json"
	"time"
)

type TaskStatus string

const (
	TaskStatusProcessing TaskStatus = "processing"
	TaskStatusSuccess    TaskStatus = "success"
	TaskStatusFailed     TaskStatus = "failed"
)

type PipelineTask struct {
	ID         int64
	PipelineID int64
	SourceURL  string
	Details    json.RawMessage
	Status     TaskStatus
	CreatedAt  time.Time
	UpdatedAt  time.Time
}
