package goflush

import (
	"context"
)

// ErrorChannel returns a read-only channel that receives errors from the Stream.
// Adds exactly 1 goroutine to watch ctx cancellation and ensure the channel is closed.
//
// IMPORTANT: upstream values are silently dropped — no value channel is returned.
func (s Stream[T]) ErrorChannel(ctx context.Context) <-chan error {
	ch, sender, closer := channel[error](ctx)
	sub := s.Observe(NewObserver[T](nil, sender, closer))

	go func() {
		<-ctx.Done()
		sub.Close()
		closer()
	}()

	return ch
}
