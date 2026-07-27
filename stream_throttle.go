package goflush

import (
	"sync"
	"time"
)

// Throttle passes the first value of each time window d and suppresses the rest.
// Uses wall-clock comparison; no goroutines added.
// Subscriber is called on the upstream goroutine (for the first event of each window).
// Errors are forwarded immediately, bypassing the throttle window.
func (s Stream[T]) Throttle(d time.Duration) *Stream[T] {
	if d <= 0 {
		return &s
	}

	return newStream(func(obs Observer[T]) Subscription {
		var mu sync.Mutex
		var lastEmit time.Time
		done := make(chan struct{})

		inner := s.Observe(NewObserver[T](
			func(v T) {
				mu.Lock()
				now := time.Now()
				if now.Sub(lastEmit) >= d {
					lastEmit = now
					mu.Unlock()
					select {
					case <-done:
					default:
						obs.Next(v)
					}
					return
				}
				mu.Unlock()
			},
			func(err error) {
				obs.Error(err)
			},
			obs.Complete,
		))

		return &throttleSubscription{inner: inner, done: done}
	})
}

type throttleSubscription struct {
	inner Subscription
	done  chan struct{}
	once  sync.Once
}

func (t *throttleSubscription) Close() {
	t.once.Do(func() {
		close(t.done)
		t.inner.Close()
	})
}
