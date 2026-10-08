package shutdown

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

type OSSource struct{}

// Wait ожидает сигнал ОС или отмену родительского контекста.
func (OSSource) Wait(ctx context.Context) error {
	signalCtx, stop := signal.NotifyContext(
		ctx,
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	<-signalCtx.Done()

	return ctx.Err()
}
