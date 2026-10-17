package stages

// ItemIDsPayload содержит список идентификаторов товаров.
type ItemIDsPayload struct {
	ItemIDs []int64 `json:"item_ids"`
}
