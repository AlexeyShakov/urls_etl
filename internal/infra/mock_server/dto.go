package mock_server

type GetItemsRequest struct {
	UserID int `json:"user_id"`
}

type GetItemsResponse struct {
	ItemIDs []int `json:"item_ids"`
}

type ItemIDsRequest struct {
	ItemIDs []int `json:"item_ids"`
}

type FillItemsResponse struct {
	FilledItems []string `json:"filled_items"`
}

type ScoreItemsResponse struct {
	Scores map[int]float64 `json:"scores"`
}

type LogItemsResponse struct {
	Logged bool `json:"logged"`
}
