package domain

import (
	"context"
	"fmt"
)

type Requester interface {
	Do(
		ctx context.Context,
		req RequestData,
	) ResponseData
}

// RequestWorker читает запросы из reqCh и выполняет их через requester.
//
// Worker завершает работу после закрытия reqCh.
// Ошибочные запросы пока только логируются.
// Позже здесь можно добавить сохранение ошибок в БД
// или отправку задач в retry/dead-letter pipeline.
func RequestWorker(workerId int, reqCh <-chan RequestData, requester Requester) {
	// TODO должны ли мы оставить цикл бесконечным?
	for req := range reqCh {
		fmt.Printf("[worker %d] started request to %s\n", workerId, req.URL)
		resp := requester.Do(context.Background(), req)
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
