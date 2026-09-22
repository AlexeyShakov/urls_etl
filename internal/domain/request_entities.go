package domain

type RequestData struct {
	URL     string
	Headers map[string]string
	Payload string
}

type ResponseData struct {
	URL        string
	StatusCode int
	Body       string
	Err        error
}

type GetItemsResponse struct {
	ItemIDs []int64 `json:"item_ids"`
}
