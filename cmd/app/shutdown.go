package main

import (
	"context"
	"log/slog"
)

type ShutdownSource interface {
	Wait(ctx context.Context) error
}

// watchShutdown отменяет контекст приложения по команде источника.
func watchShutdown(
	ctx context.Context,
	cancel context.CancelFunc,
	source ShutdownSource,
) {
	if err := source.Wait(ctx); err != nil {
		if ctx.Err() == nil {
			slog.Error("shutdown source failed", "err", err)
		}
		return
	}

	cancel()
}
