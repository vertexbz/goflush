package goflush

import (
	"errors"
)

// MapErr transforms each value using fn, returning a Stream of the new type B.
// Errors are forwarded downstream; fn is not called for errors.
// No goroutines added.
func (s Stream[T]) MapErr[B any](fn func(T) (B, error)) *Stream[B] {
	return newStream(func(obs Observer[B]) Subscription {
		return s.Observe(NewObserver[T](
			func(v T) {
				event, err := fn(v)
				if err != nil {
					if !errors.Is(err, Drop) {
						obs.Error(err)
					}
				} else {
					obs.Next(event)
				}
			},
			obs.Error,
			obs.Complete,
		))
	})
}
