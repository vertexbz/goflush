package goflush

// Map transforms each value using fn, returning a Stream of the new type B.
// Errors are forwarded downstream; fn is not called for errors.
// No goroutines added.
func (s Stream[T]) Map[B any](fn func(T) B) *Stream[B] {
	return newStream(func(obs Observer[B]) Subscription {
		return s.Observe(NewObserver[T](
			func(v T) {
				obs.Next(fn(v))
			},
			obs.Error,
			obs.Complete,
		))
	})
}
