package domain_new

type ItemsRequestPayload struct {
	ItemIDs []int64 `json:"item_ids"`
}

type GetItemsResponse struct {
	ItemIDs []int64 `json:"item_ids"`
}
