package domain

type Stage string

const (
	StageGetItems   Stage = "get_items"
	StageFillItems  Stage = "fill_items"
	StageScoreItems Stage = "score_items"
	StageLogItems   Stage = "log_items"
)

type TaskStatus string

const (
	TaskStatusProcessing TaskStatus = "processing"
	TaskStatusSuccess    TaskStatus = "success"
	TaskStatusFailed     TaskStatus = "failed"
)

type PipelineStatus string

const (
	PipelineStatusPending    PipelineStatus = "pending"
	PipelineStatusProcessing PipelineStatus = "processing"
	PipelineStatusFinished   PipelineStatus = "finished"
	PipelineStatusStopped    PipelineStatus = "stopped"
)

type StageStatus string

const (
	StageStatusSuccess StageStatus = "success"
	StageStatusFail    StageStatus = "failed"
)
