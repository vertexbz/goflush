package goflush

// Filter returns a Stream that emits only values for which pred returns true.
// Errors are forwarded unchanged; pred is not called for errors.
// No goroutines added. Subscriber is called on the same goroutine as upstream.
func (s Stream[T]) Filter(pred func(T) bool) *Stream[T] {
	return newStream(func(obs Observer[T]) Subscription {
		return s.Observe(NewObserver[T](
			func(v T) {
				if pred(v) {
					obs.Next(v)
				}
			},
			obs.Error,
			obs.Complete,
		))
	})
}
