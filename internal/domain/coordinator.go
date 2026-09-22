package domain

import "sync"

type PipelineCoordinator struct {
	counter                       int
	initialUrlsSubmissionFinished bool
	mu                            sync.Mutex
	cond                          *sync.Cond
}

func (p *PipelineCoordinator) Add(n int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.counter += n
}

// Done отмечает завершение одной задачи пайплайна.
func (p *PipelineCoordinator) Done() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.counter == 0 {
		panic("pipeline coordinator: counter cannot be negative")
	}

	p.counter--

	p.cond.Broadcast()
}

func (p *PipelineCoordinator) FinishInitialUrlsSubmission() {
	//todo а точно ли здесь нужно брать блокировку?
	p.mu.Lock()
	defer p.mu.Unlock()

	p.initialUrlsSubmissionFinished = true

	p.cond.Broadcast()
}

func (p *PipelineCoordinator) Wait() {
	p.mu.Lock()
	defer p.mu.Unlock()
	// todo нужен механизм, чтобы не зависнули навсегда
	for !p.initialUrlsSubmissionFinished || p.counter != 0 {
		p.cond.Wait()
	}
}

func NewPipelineCoordinator() *PipelineCoordinator {
	coordinator := &PipelineCoordinator{}
	coordinator.cond = sync.NewCond(&coordinator.mu)
	return coordinator
}
