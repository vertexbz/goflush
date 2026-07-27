package goflush

import (
	"context"
	"sync"
)

// NewSource is the producer-function form.
// The producer goroutine starts when the first subscriber subscribes and stops
// when the last subscriber unsubscribes or the producer returns. While running,
// every emitted value and error is broadcast to all current subscribers.
func NewSource[T any](ctx context.Context, produce func(emit func(T), emitErr func(error))) *Stream[T] {
	sh := &sourceShared[T]{}

	return newStream(func(obs Observer[T]) Subscription {
		isFirst := sh.add(obs)

		if isFirst {
			ctx2, cancel := context.WithCancel(ctx)
			sh.mu.Lock()
			sh.cancel = cancel
			sh.mu.Unlock()

			go func() {
				defer cancel()
				produce(
					func(v T) {
						select {
						case <-ctx2.Done():
						default:
							for _, o := range sh.snapshot() {
								o.Next(v)
							}
						}
					},
					func(err error) {
						select {
						case <-ctx2.Done():
						default:
							for _, o := range sh.snapshot() {
								o.Error(err)
							}
						}
					},
				)
				// Producer finished: notify all remaining subscribers then clear the list.
				sh.mu.Lock()
				remaining := make([]Observer[T], len(sh.observers))
				copy(remaining, sh.observers)
				sh.observers = nil
				sh.mu.Unlock()
				for _, o := range remaining {
					o.Complete()
				}
			}()
		}

		return &sourceSubscription[T]{obs: obs, shared: sh}
	})
}

// sourceShared holds the shared state for a multicast NewSource stream.
type sourceShared[T any] struct {
	mu        sync.Mutex
	observers []Observer[T]
	cancel    context.CancelFunc
}

func (sh *sourceShared[T]) add(obs Observer[T]) bool {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	sh.observers = append(sh.observers, obs)
	return len(sh.observers) == 1
}

func (sh *sourceShared[T]) remove(obs Observer[T]) {
	sh.mu.Lock()
	for i, o := range sh.observers {
		if o == obs {
			sh.observers = append(sh.observers[:i], sh.observers[i+1:]...)
			break
		}
	}
	last := len(sh.observers) == 0
	cancel := sh.cancel
	sh.mu.Unlock()
	if last && cancel != nil {
		cancel()
	}
}

func (sh *sourceShared[T]) snapshot() []Observer[T] {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	cp := make([]Observer[T], len(sh.observers))
	copy(cp, sh.observers)
	return cp
}
