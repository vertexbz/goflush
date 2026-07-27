package goflush

// ErrorFilter returns a Stream that emits only errors for which pred returns true.
// Values are forwarded unchanged; pred is not called for values.
// No goroutines added. Subscriber is called on the same goroutine as upstream.
func (s Stream[T]) ErrorFilter(pred func(error) bool) *Stream[T] {
	return newStream(func(obs Observer[T]) Subscription {
		return s.Observe(NewObserver[T](
			obs.Next,
			func(v error) {
				if pred(v) {
					obs.Error(v)
				}
			},
			obs.Complete,
		))
	})
}
