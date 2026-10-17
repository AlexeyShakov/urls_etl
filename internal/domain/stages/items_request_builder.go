package stages

import (
	"encoding/json"

	"urls_etl/internal/domain/pipeline"
)

// ItemsRequestBuilder формирует запрос следующей стадии для списка товаров.
type ItemsRequestBuilder func(
	data pipeline.PipelineData,
	itemIDs []int64,
) (pipeline.PipelineData, error)

// BuildFillItemsRequest формирует запрос стадии FillItems.
func BuildFillItemsRequest(
	data pipeline.PipelineData,
	itemIDs []int64,
) (pipeline.PipelineData, error) {
	return buildItemsRequest(
		data,
		pipeline.StageFillItems,
		"http://localhost:8080/service1/fillItems",
		itemIDs,
	)
}

// BuildScoreItemsRequest формирует запрос стадии ScoreItems.
func BuildScoreItemsRequest(
	data pipeline.PipelineData,
	itemIDs []int64,
) (pipeline.PipelineData, error) {
	return buildItemsRequest(
		data,
		pipeline.StageScoreItems,
		"http://localhost:8080/service2/scoreItems",
		itemIDs,
	)
}

// BuildLogItemsRequest формирует запрос стадии LogItems.
func BuildLogItemsRequest(
	data pipeline.PipelineData,
	itemIDs []int64,
) (pipeline.PipelineData, error) {
	return buildItemsRequest(
		data,
		pipeline.StageLogItems,
		"http://localhost:8080/service3/logItems",
		itemIDs,
	)
}

// buildItemsRequest формирует JSON-запрос, сохраняя идентификаторы задачи и пайплайна.
func buildItemsRequest(
	data pipeline.PipelineData,
	stage pipeline.Stage,
	url string,
	itemIDs []int64,
) (pipeline.PipelineData, error) {
	payload, err := json.Marshal(ItemIDsPayload{
		ItemIDs: itemIDs,
	})
	if err != nil {
		return pipeline.PipelineData{}, err
	}

	return pipeline.PipelineData{
		TaskID:     data.TaskID,
		PipelineID: data.PipelineID,
		Stage:      stage,
		Request: pipeline.RequestData{
			URL: url,
			Headers: map[string]string{
				"Content-Type": "application/json",
			},
			Payload: string(payload),
		},
	}, nil
}
