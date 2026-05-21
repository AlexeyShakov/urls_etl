package main

import (
	"sync"
	"urls_etl/internal/config"
	"urls_etl/internal/domain"
	infra_http "urls_etl/internal/infra/http"
)

var urls = []domain.RequestData{
	{
		URL: "https://example.com/api/users",
		Headers: map[string]string{
			"Authorization": "Bearer token-1",
			"Content-Type":  "application/json",
		},
		Payload: `{"user_id": 1}`,
	},
	{
		URL: "https://example.com/api/orders",
		Headers: map[string]string{
			"Authorization": "Bearer token-2",
			"Content-Type":  "application/json",
		},
		Payload: `{"order_id": 100}`,
	},
}

func main() {
	workerCfg := config.NewDefaultWorkersConfig()
	httpCfg := config.NewDefaultHTTPConfig()

	// Канал, куда кладется информация о внешних запросах. На схеме это next_channel
	requestChannel := make(chan domain.RequestData, workerCfg.RequestChannelLen)
	// Каналы, куда складываются данные для запроса. На схеме это  in channel
	firstWorkerCh := make(chan domain.RequestData, workerCfg.WorkerChannelLen)
	secondWorkerCh := make(chan domain.RequestData, workerCfg.WorkerChannelLen)

	client := infra_http.NewHTTPClient(httpCfg)
	httpRequester := infra_http.NewHttpRequester(client, httpCfg)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		domain.DispatchRequests(requestChannel, firstWorkerCh, secondWorkerCh)
	}()
	// TODO можно ли передачу каналов и селекс сделать динамически?
	wg.Add(2)
	go func() {
		defer wg.Done()
		domain.RequestWorker(1, firstWorkerCh, httpRequester)
	}()
	go func() {
		defer wg.Done()
		domain.RequestWorker(2, secondWorkerCh, httpRequester)
	}()

	for _, req := range urls {
		requestChannel <- req
	}
	close(requestChannel)
	wg.Wait()
}
