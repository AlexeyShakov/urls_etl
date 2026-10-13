package pipeline

import (
	"context"
	"sync"
)

// PipelineCoordinator отслеживает количество незавершенных работ пайплайна.
type PipelineCoordinator struct {
	counter                       int
	initialUrlsSubmissionFinished bool
	mu                            sync.Mutex
	cond                          *sync.Cond
}

// NewPipelineCoordinator создает координатор пайплайна.
func NewPipelineCoordinator() *PipelineCoordinator {
	coordinator := &PipelineCoordinator{}
	coordinator.cond = sync.NewCond(&coordinator.mu)

	return coordinator
}

// Add увеличивает количество незавершенных работ.
func (p *PipelineCoordinator) Add(n int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.counter += n
}

// Done отмечает завершение одной работы.
func (p *PipelineCoordinator) Done() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.counter == 0 {
		panic("pipeline coordinator: counter cannot be negative")
	}

	p.counter--
	p.cond.Broadcast()
}

// FinishInitialUrlsSubmission отмечает завершение отправки начальных задач.
func (p *PipelineCoordinator) FinishInitialUrlsSubmission() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.initialUrlsSubmissionFinished = true
	p.cond.Broadcast()
}

// Wait ожидает завершения всех работ или отмены контекста.
func (p *PipelineCoordinator) Wait(ctx context.Context) error {
	stop := context.AfterFunc(ctx, func() {
		p.mu.Lock()
		defer p.mu.Unlock()

		// Блокировка предотвращает потерю уведомления перед cond.Wait().
		p.cond.Broadcast()
	})
	defer stop()

	p.mu.Lock()
	defer p.mu.Unlock()

	for !p.initialUrlsSubmissionFinished || p.counter != 0 {
		if err := ctx.Err(); err != nil {
			return err
		}

		p.cond.Wait()
	}

	return ctx.Err()
}
