package domain

import (
	"encoding/json"
	"time"
)

type Pipeline struct {
	ID         int64
	Status     Status
	CreatedAt  time.Time
	FinishedAt *time.Time
	UpdatedAt  time.Time
}

type PipelineTask struct {
	ID         int64
	PipelineID int64
	SourceURL  string
	Details    json.RawMessage
	Status     Status
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

type StageResult struct {
	ID        int64
	TaskID    int64
	Stage     Stage
	Status    Status
	Attempt   int
	Details   json.RawMessage
	CreatedAt time.Time
	UpdatedAt time.Time
}

type RequestDetails struct {
	Payload string
	Headers map[string]string
}

// todo может получиться как-то получше назвать?
type PipelineData struct {
	TaskID     int64
	PipelineID int64
	Request    RequestData
	Stage      Stage
}

type RequestResult struct {
	PipelineData PipelineData
	Response     ResponseData
}
