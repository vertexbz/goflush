package goflush

import (
	"reflect"
	"sync"
)

// Distinct suppresses consecutive duplicate values.
// Uses interface{} (any) comparison — T must be comparable at runtime.
// Errors are forwarded unchanged. No goroutines added.
// Subscriber is called on the same goroutine as upstream.
func (s Stream[T]) Distinct() *Stream[T] {
	return newStream(func(obs Observer[T]) Subscription {
		var last any
		var hasLast bool
		var mu sync.Mutex

		return s.Observe(NewObserver[T](
			func(v T) {
				mu.Lock()
				if hasLast && reflect.DeepEqual(last, v) {
					mu.Unlock()
					return
				}
				last = any(v)
				hasLast = true
				mu.Unlock()
				obs.Next(v)
			},
			obs.Error,
			obs.Complete,
		))
	})
}
