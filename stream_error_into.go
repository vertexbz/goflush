package goflush

import (
	"context"
	"sync"
)

// ErrorInto pushes errors from the Stream into ch until the Stream completes or ctx is cancelled.
// Blocks the caller's goroutine. Adds 0 goroutines.
//
// IMPORTANT: upstream values are silently dropped.
func (s Stream[T]) ErrorInto(ctx context.Context, ch chan<- error) {
	done := make(chan struct{})
	var once sync.Once
	closeDone := func() { once.Do(func() { close(done) }) }

	sub := s.Observe(NewObserver[T](
		nil,
		func(v error) {
			select {
			case <-ctx.Done():
			case ch <- v:
			}
		},
		func() { closeDone() },
	))

	select {
	case <-ctx.Done():
		sub.Close()
	case <-done:
	}
}
