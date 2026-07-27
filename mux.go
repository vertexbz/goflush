package goflush

import (
	"errors"
	"sync"
)

// Mux combines multiple streams into one using a muxing function.
// Whenever any source emits a value, fn is called with pointers to the last known
// value of each source (passed as a variadic spread); sources that have not yet
// emitted any value contribute a nil pointer.
// The return value of fn is emitted downstream.
// Errors from any source are forwarded downstream.
// No goroutines are added by Mux itself.
// A mutex ensures safe concurrent access when sources run on different goroutines.
// Subscriber is called on the same goroutine as the emitting source.
func Mux[T, R any](fn func(values ...*T) R, sources ...*Stream[T]) *Stream[R] {
	return MuxErr(func(values ...*T) (R, error) {
		return fn(values...), nil
	}, sources...)
}

// MuxErr is like Mux but the combining function may also return an error.
// If fn returns a non-nil error that is not Drop, the error is forwarded downstream.
// If fn returns Drop, the event is silently discarded without emitting a value or error.
// A mutex ensures safe concurrent access when sources run on different goroutines.
func MuxErr[T, R any](fn func(values ...*T) (R, error), sources ...*Stream[T]) *Stream[R] {
	return newStream(func(obs Observer[R]) Subscription {
		if len(sources) == 0 {
			obs.Complete()
			return noopSubscription{}
		}

		var mu sync.Mutex
		lasts := make([]*T, len(sources))
		subs := make([]Subscription, len(sources))

		for i, src := range sources {
			i := i
			subs[i] = src.Observe(NewObserver[T](
				func(v T) {
					mu.Lock()
					lasts[i] = &v
					args := make([]*T, len(lasts))
					copy(args, lasts)
					mu.Unlock()

					if result, err := fn(args...); err != nil {
						if !errors.Is(err, Drop) {
							obs.Error(err)
						}
					} else {
						obs.Next(result)
					}
				},
				func(err error) {
					mu.Lock()
					defer mu.Unlock()
					obs.Error(err)
				},
				nil,
			))
		}
		return &listSubscription{subs: subs}
	})
}
