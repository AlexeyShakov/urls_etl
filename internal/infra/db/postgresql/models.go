package postgresql

import (
	"encoding/json"
	"time"

	"github.com/uptrace/bun"
)

type pipelineModel struct {
	bun.BaseModel `bun:"table:pipelines"`

	ID         int64 `bun:",pk,autoincrement"`
	Status     string
	FinishedAt *time.Time
}

type pipelineTaskModel struct {
	bun.BaseModel `bun:"table:pipeline_tasks"`

	ID         int64 `bun:",pk,autoincrement"`
	PipelineID int64
	SourceURL  string
	Details    json.RawMessage
	Status     string
}

type stageResultModel struct {
	bun.BaseModel `bun:"table:pipeline_stage_results"`

	ID      int64 `bun:",pk,autoincrement"`
	TaskID  int64
	Stage   string
	Status  string
	Attempt int
	Details json.RawMessage
}
