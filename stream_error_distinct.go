package goflush

import (
	"errors"
	"sync"
)

// ErrorDistinct suppresses consecutive duplicate errors, using errors.Is for comparison.
// Values are forwarded unchanged.
// A mutex guards the last-seen error so concurrent upstream sources are safe.
// No goroutines added. Subscriber is called on the same goroutine as upstream.
func (s Stream[T]) ErrorDistinct() *Stream[T] {
	return newStream(func(obs Observer[T]) Subscription {
		var mu sync.Mutex
		var last error
		return s.Observe(NewObserver[T](
			obs.Next,
			func(v error) {
				mu.Lock()
				if last != nil && errors.Is(v, last) {
					mu.Unlock()
					return
				}
				last = v
				mu.Unlock()
				obs.Error(v)
			},
			obs.Complete,
		))
	})
}
