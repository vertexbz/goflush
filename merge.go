package goflush

import "sync"

// Merge combines multiple Sources into one. Values from each source are forwarded
// as they arrive (on each source's own goroutine). Errors from any source are forwarded.
// No goroutines are added by Merge itself.
// A mutex ensures safe concurrent access when sources run on different goroutines.
func Merge[T any](sources ...*Stream[T]) *Stream[T] {
	return newStream(func(obs Observer[T]) Subscription {
		if len(sources) == 0 {
			obs.Complete()
			return noopSubscription{}
		}

		var mu sync.Mutex
		remaining := len(sources)
		subs := make([]Subscription, len(sources))

		safeNext := func(v T) {
			mu.Lock()
			defer mu.Unlock()
			obs.Next(v)
		}
		safeError := func(err error) {
			mu.Lock()
			defer mu.Unlock()
			obs.Error(err)
		}
		safeComplete := func() {
			mu.Lock()
			remaining--
			done := remaining == 0
			mu.Unlock()
			if done {
				obs.Complete()
			}
		}

		for i, src := range sources {
			subs[i] = src.Observe(NewObserver[T](safeNext, safeError, safeComplete))
		}
		return &listSubscription{subs: subs}
	})
}
