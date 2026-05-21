package domain

import "fmt"

// DispatchRequests распределяет входящие запросы между worker-ами.
//
// Функция читает запросы из reqCh до тех пор, пока канал не будет закрыт.
// Каждый запрос отправляется в первый доступный worker channel.
// После закрытия reqCh функция закрывает worker channels,
// так как именно она является их единственным sender-ом.
func DispatchRequests(reqCh <-chan RequestData, firstWorkerCh, secondWorkerCh chan<- RequestData) {
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
