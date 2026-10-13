package domain_new

import (
	"encoding/json"
	"time"
)

type Stage string

const (
	StageGetItems   Stage = "get_items"
	StageFillItems  Stage = "fill_items"
	StageScoreItems Stage = "score_items"
	StageLogItems   Stage = "log_items"
)

type StageStatus string

const (
	StageStatusSuccess StageStatus = "success"
	StageStatusFail    StageStatus = "failed"
)

// StageResult хранит результат одной попытки выполнения стадии задачи:
// статус, номер попытки и подробности запроса, ответа или ошибки.
type StageResult struct {
	ID        int64
	TaskID    int64
	Stage     Stage
	Status    StageStatus
	Attempt   int
	Details   json.RawMessage
	CreatedAt time.Time
	UpdatedAt time.Time
}

// PipelineData содержит данные для выполнения одной стадии задачи.
type PipelineData struct {
	TaskID     int64
	PipelineID int64
	Request    RequestData
	Stage      Stage
}

// RequestResult содержит исходный запрос стадии и результат его выполнения.
type RequestResult struct {
	PipelineData PipelineData
	Response     ResponseData
}
