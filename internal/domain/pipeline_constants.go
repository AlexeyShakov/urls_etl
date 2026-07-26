package domain

type Stage string

const (
	StageGetItems   Stage = "get_items"
	StageFillItems  Stage = "fill_items"
	StageScoreItems Stage = "score_items"
	StageLogItems   Stage = "log_items"
	StageSaveResult Stage = "save_result"
)

type Status string

const (
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusSuccess    Status = "success"
	StatusFailed     Status = "failed"
)
