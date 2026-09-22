package domain

import (
	"context"
	"fmt"
)

// DispatchRequests распределяет входящие запросы между worker-ами.
//
// Функция читает запросы из reqCh до тех пор, пока канал не будет закрыт.
// Каждый запрос отправляется в первый доступный worker channel.
// После закрытия reqCh функция закрывает worker channels,
// так как именно она является их единственным sender-ом.
func DispatchRequests(ctx context.Context, reqCh <-chan PipelineData, firstWorkerCh, secondWorkerCh chan<- PipelineData) {
	defer func() {
		close(firstWorkerCh)
		close(secondWorkerCh)
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case req, ok := <-reqCh:
			if !ok {
				return
			}
			select {
			case firstWorkerCh <- req:
				fmt.Println("Sent to firstWorkerCh")
			case secondWorkerCh <- req:
				fmt.Println("Sent to secondWorkerCh")
			case <-ctx.Done():
				return
			}
		}
	}
}
