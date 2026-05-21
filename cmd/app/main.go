package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

type requestData struct {
	URL     string
	Headers map[string]string
	Payload string
}

type responseData struct {
	URL        string
	StatusCode int
	Body       string
	Err        error
}

var urls = []requestData{
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
	// Канал, куда кладется информация о внешних запросах. На схеме это next_channel
	requestChannel := make(chan requestData, RequestChannelLen)
	// Каналы, куда складываются данные для запроса. На схеме это  in channel
	firstWorkerCh := make(chan requestData, WorkerChannelLen)
	secondWorkerCh := make(chan requestData, WorkerChannelLen)

	client := newHTTPClient()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		dispatchRequests(requestChannel, firstWorkerCh, secondWorkerCh)
	}()
	// TODO можно ли передачу каналов и селекс сделать динамически?
	wg.Add(2)
	go func() {
		defer wg.Done()
		requestWorker(1, firstWorkerCh, client)
	}()
	go func() {
		defer wg.Done()
		requestWorker(2, secondWorkerCh, client)
	}()

	for _, req := range urls {
		requestChannel <- req
	}
	close(requestChannel)
	wg.Wait()
}

func dispatchRequests(reqCh <-chan requestData, firstWorkerCh, secondWorkerCh chan<- requestData) {
	// Это work link asker на схеме
	// TODO должны ли мы оставить цикл бесконечным?
	for req := range reqCh {
		select {
		case firstWorkerCh <- req:
			fmt.Println("Sent to firstWorkerCh") // TODO что мы должны поставить сюда вместо printc
		case secondWorkerCh <- req:
			fmt.Println("Sent to secondWorkerCh") // TODO что мы должны поставить сюда вместо printc
		}
	}
	close(firstWorkerCh)
	close(secondWorkerCh)

}

func requestWorker(workerId int, reqCh <-chan requestData, client *http.Client) {
	// TODO должны ли мы оставить цикл бесконечным?
	for req := range reqCh {
		fmt.Printf("[worker %d] started request to %s\n", workerId, req.URL)
		resp := makeRequestWithRetry(context.Background(), client, req)
		if resp.Err != nil {
			fmt.Printf("[worker %d] failed request to %s: %v\n", workerId, req.URL, resp.Err)
			// TODO нужно заносить в БД
			continue
		}
		fmt.Printf(
			"[worker %d] finished request to %s with status %d\n",
			workerId,
			resp.URL,
			resp.StatusCode,
		)
		// TODO будем передавать response дальше
	}
}

func newHTTPClient() *http.Client {
	transport := &http.Transport{
		// Максимальное количество одновременно открытых соединений к одному хосту.
		MaxConnsPerHost: MaxConnsPerHost,
		// Максимальное количество idle (неиспользуемых) keep-alive соединений для всех хостов.
		MaxIdleConns: MaxIdleConns,
		// Максимальное количество idle keep-alive соединений на один хост.
		MaxIdleConnsPerHost: MaxIdleConnsPerHost,
		// Через какое время неиспользуемое соединение будет закрыто.
		IdleConnTimeout: IdleConnTimeout,
	}

	return &http.Client{
		Transport: transport,
		Timeout:   RequestTimeout,
	}
}

func makeRequestWithRetry(
	ctx context.Context,
	client *http.Client,
	reqData requestData,
) responseData {
	var lastErr error

	for attempt := 1; attempt <= MaxRetries; attempt++ {
		resp, err := makeRequest(ctx, client, reqData)
		if err == nil && resp.StatusCode < 500 {
			return resp
		}

		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("server error: status code %d", resp.StatusCode)
		}

		time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
	}

	return responseData{
		URL: reqData.URL,
		Err: lastErr,
	}
}

func makeRequest(
	ctx context.Context,
	client *http.Client,
	reqData requestData,
) (responseData, error) {
	httpReq, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		reqData.URL,
		bytes.NewBufferString(reqData.Payload),
	)
	if err != nil {
		return responseData{URL: reqData.URL, Err: err}, err
	}

	for key, value := range reqData.Headers {
		httpReq.Header.Set(key, value)
	}

	httpResp, err := client.Do(httpReq)
	if err != nil {
		return responseData{URL: reqData.URL, Err: err}, err
	}
	defer func() {
		_ = httpResp.Body.Close()
	}()

	bodyBytes, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return responseData{URL: reqData.URL, StatusCode: httpResp.StatusCode, Err: err}, err
	}

	return responseData{
		URL:        reqData.URL,
		StatusCode: httpResp.StatusCode,
		Body:       string(bodyBytes),
	}, nil
}
