package goflush

import (
	"context"
	"sync"
)

// Channel returns a read-only channel that receives values from the Stream.
// Adds exactly 1 goroutine to watch ctx cancellation and ensure the channel is closed.
//
// IMPORTANT: upstream errors are silently dropped — no error channel is returned.
func (s Stream[T]) Channel(ctx context.Context) <-chan T {
	ch, sender, closer := channel[T](ctx)
	sub := s.Observe(NewObserver[T](sender, nil, closer))

	go func() {
		<-ctx.Done()
		sub.Close()
		closer()
	}()

	return ch
}

func channel[T any](ctx context.Context) (<-chan T, func(T), func()) {
	ch := make(chan T, 16)
	var mu sync.RWMutex
	closed := false

	sendValue := func(v T) {
		mu.RLock()
		defer mu.RUnlock()
		if !closed {
			select {
			case <-ctx.Done():
			case ch <- v:
			}
		}
	}

	closeCh := func() {
		mu.Lock()
		defer mu.Unlock()
		if !closed {
			closed = true
			close(ch)
		}
	}

	return ch, sendValue, closeCh
}
