package goflush

// FlatMap maps each value to a Stream[B] and subscribes to it inline.
// Errors from the outer source and from each inner source are forwarded.
// No goroutines added.
func (s Stream[T]) FlatMap[B any](fn func(T) *Stream[B]) *Stream[B] {
	return newStream(func(obs Observer[B]) Subscription {
		return s.Observe(NewObserver[T](
			func(v T) {
				inner := fn(v)
				inner.Observe(NewObserver[B](obs.Next, obs.Error, nil))
			},
			obs.Error,
			obs.Complete,
		))
	})
}
