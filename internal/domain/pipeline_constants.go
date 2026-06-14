package domain

const (
	StageGetItems   string = "get_items"
	StageFillItems  string = "fill_items"
	StageScoreItems string = "score_items"
	StageLogItems   string = "log_items"
	StageSaveResult string = "save_result"
)

const (
	StatusPending    string = "pending"
	StatusProcessing string = "processing"
	StatusSuccess    string = "success"
	StatusFailed     string = "failed"
)
