package domain

import "encoding/json"

type ItemsRequestBuilder func(
	data PipelineData,
	itemIDs []int64,
) (PipelineData, error)

// BuildFillItemsRequest формирует запрос для стадии FillItems.
func BuildFillItemsRequest(
	data PipelineData,
	itemIDs []int64,
) (PipelineData, error) {
	return buildItemsRequest(
		data,
		StageFillItems,
		"http://localhost:8080/fillItems",
		itemIDs,
	)
}

// BuildScoreItemsRequest формирует запрос для стадии ScoreItems.
func BuildScoreItemsRequest(
	data PipelineData,
	itemIDs []int64,
) (PipelineData, error) {
	return buildItemsRequest(
		data,
		StageScoreItems,
		"http://localhost:8080/scoreItems",
		itemIDs,
	)
}

// BuildLogItemsRequest формирует запрос для стадии LogItems.
func BuildLogItemsRequest(
	data PipelineData,
	itemIDs []int64,
) (PipelineData, error) {
	return buildItemsRequest(
		data,
		StageLogItems,
		"http://localhost:8080/logItems",
		itemIDs,
	)
}

// buildItemsRequest формирует запрос для обработки списка товаров на следующей стадии.
func buildItemsRequest(
	data PipelineData,
	stage Stage,
	url string,
	itemIDs []int64,
) (PipelineData, error) {
	payload, err := json.Marshal(ItemsRequestPayload{ItemIDs: itemIDs})
	if err != nil {
		return PipelineData{}, err
	}

	return PipelineData{
		TaskID:     data.TaskID,
		PipelineID: data.PipelineID,
		Stage:      stage,
		Request: RequestData{
			URL: url,
			Headers: map[string]string{
				"Content-Type": "application/json",
			},
			Payload: string(payload),
		},
	}, nil
}
