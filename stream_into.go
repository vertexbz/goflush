package goflush

import (
	"context"
	"sync"
)

// Into pushes values from the Stream into ch until the Stream completes or ctx is cancelled.
// Blocks the caller's goroutine. Adds 0 goroutines.
//
// IMPORTANT: upstream errors are silently dropped.
func (s Stream[T]) Into(ctx context.Context, ch chan<- T) {
	done := make(chan struct{})
	var once sync.Once
	closeDone := func() { once.Do(func() { close(done) }) }

	sub := s.Observe(NewObserver[T](
		func(v T) {
			select {
			case <-ctx.Done():
			case ch <- v:
			}
		},
		nil,
		func() { closeDone() },
	))

	select {
	case <-ctx.Done():
		sub.Close()
	case <-done:
	}
}
