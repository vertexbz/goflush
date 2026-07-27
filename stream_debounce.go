package goflush

import (
	"sync"
	"time"
)

// Debounce emits a value only after d has elapsed with no new upstream values.
// Adds exactly 1 goroutine per subscription (via time.AfterFunc).
// Subscriber is called on the timer's goroutine.
// Errors are forwarded immediately, bypassing the debounce timer.
func (s Stream[T]) Debounce(d time.Duration) *Stream[T] {
	if d <= 0 {
		return &s
	}

	return newStream(func(obs Observer[T]) Subscription {
		var mu sync.Mutex
		var timer *time.Timer
		done := make(chan struct{})

		inner := s.Observe(NewObserver[T](
			func(v T) {
				mu.Lock()
				if timer != nil {
					timer.Stop()
				}
				timer = time.AfterFunc(d, func() {
					select {
					case <-done:
					default:
						obs.Next(v)
					}
				})
				mu.Unlock()
			},
			func(err error) {
				obs.Error(err)
			},
			func() {
				mu.Lock()
				if timer != nil {
					timer.Stop()
					timer = nil
				}
				mu.Unlock()
				obs.Complete()
			},
		))

		return &debounceSubscription{inner: inner, done: done}
	})
}

type debounceSubscription struct {
	inner Subscription
	done  chan struct{}
	once  sync.Once
}

func (d *debounceSubscription) Close() {
	d.once.Do(func() {
		close(d.done)
		d.inner.Close()
	})
}
