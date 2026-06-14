package domain

import (
	"encoding/json"
	"time"
)

//todo нужно отрефакторить файл и возможно всю директорию, разнести все на под-директории

type Pipeline struct {
	ID         int64
	Status     string
	CreatedAt  time.Time
	FinishedAt *time.Time
	UpdatedAt  time.Time
}

type PipelineTask struct {
	ID         int64
	PipelineID int64
	SourceURL  string
	Details    json.RawMessage
	Status     string
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

type StageResult struct {
	ID        int64
	TaskID    int64
	Stage     string
	Status    string
	Attempt   int
	Details   json.RawMessage
	CreatedAt time.Time
	UpdatedAt time.Time
}

type RequestDetails struct {
	Payload string
	Headers map[string]string
}

type PipelineData struct {
	TaskID     int64
	PipelineID int64
	Request    RequestData
}
