package goflush

// Scan accumulates state using fn, emitting the running accumulator after each value.
// seed is the initial accumulator value. Errors are forwarded; fn is not called for errors.
// No goroutines added. Subscriber is called on the same goroutine as upstream.
func (s Stream[T]) Scan[B any](seed B, fn func(B, T) B) *Stream[B] {
	return newStream(func(obs Observer[B]) Subscription {
		acc := seed
		return s.Observe(NewObserver[T](
			func(v T) {
				acc = fn(acc, v)
				obs.Next(acc)
			},
			obs.Error,
			obs.Complete,
		))
	})
}
