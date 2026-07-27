package goflush

// ErrorCatch intercepts an upstream error and replaces it with the Stream returned by fn.
// The replacement source is subscribed inline (no goroutines added).
// Errors from the fallback source propagate downstream unchanged.
func (s Stream[T]) ErrorCatch(fn func(error) *Stream[T]) *Stream[T] {
	return newStream(func(obs Observer[T]) Subscription {
		return s.Observe(NewObserver[T](
			obs.Next,
			func(err error) {
				fallback := fn(err)
				fallback.Observe(obs)
			},
			obs.Complete,
		))
	})
}
