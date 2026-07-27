package goflush

import "sync"

// Series subscribes to each source in order, forwarding its values and errors downstream.
// The next source is subscribed only after the previous one completes.
// The downstream Complete is called after the last source completes.
// No goroutines are added. All callbacks are called on each source's own goroutine.
func Series[T any](sources ...*Stream[T]) *Stream[T] {
	return newStream(func(obs Observer[T]) Subscription {
		if len(sources) == 0 {
			obs.Complete()
			return noopSubscription{}
		}

		var mu sync.Mutex
		var current Subscription
		index := 0

		var subscribeNext func()
		subscribeNext = func() {
			mu.Lock()
			i := index
			if i >= len(sources) {
				mu.Unlock()
				obs.Complete()
				return
			}
			index++
			mu.Unlock()

			sub := sources[i].Observe(NewObserver[T](
				obs.Next,
				obs.Error,
				subscribeNext,
			))

			mu.Lock()
			current = sub
			mu.Unlock()
		}

		subscribeNext()

		return &seriesSubscription{mu: &mu, current: &current}
	})
}
