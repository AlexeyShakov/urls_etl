package requests

import (
	"sync/atomic"

	"urls_etl/internal/domain/pipeline"
)

// ChannelCoordinator закрывает канал после завершения всех отправителей.
type ChannelCoordinator struct {
	remaining atomic.Int64
	resultCh  chan<- pipeline.RequestResult
}

// NewChannelCoordinator создает координатор для заданного количества воркеров.
func NewChannelCoordinator(
	workersCount int,
	resultCh chan<- pipeline.RequestResult,
) *ChannelCoordinator {
	if workersCount <= 0 {
		panic("channel coordinator: workers count must be positive")
	}
	if resultCh == nil {
		panic("channel coordinator: result channel must not be nil")
	}

	coordinator := &ChannelCoordinator{
		resultCh: resultCh,
	}
	coordinator.remaining.Store(int64(workersCount))

	return coordinator
}

// Done отмечает завершение воркера и закрывает канал после последнего отправителя.
func (c *ChannelCoordinator) Done() {
	remaining := c.remaining.Add(-1)

	if remaining < 0 {
		panic("channel coordinator: too many completion notifications")
	}

	if remaining == 0 {
		close(c.resultCh)
	}
}
