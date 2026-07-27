package goflush

// ErrorMap transforms an upstream error using fn and re-emits it as an error downstream.
// Values pass through unchanged. No goroutines added.
func (s Stream[T]) ErrorMap(fn func(error) error) *Stream[T] {
	return newStream(func(obs Observer[T]) Subscription {
		return s.Observe(NewObserver[T](
			obs.Next,
			func(err error) {
				obs.Error(fn(err))
			},
			obs.Complete,
		))
	})
}
