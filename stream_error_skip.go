package goflush

// ErrorSkip returns a Stream that ignores the first n errors, then forwards the rest.
// Values are always forwarded immediately and do not count toward the skip budget.
// No goroutines added.
func (s Stream[T]) ErrorSkip(n int) *Stream[T] {
	if n <= 0 {
		return &s
	}

	return newStream(func(obs Observer[T]) Subscription {
		skipped := 0
		return s.Observe(NewObserver[T](
			obs.Next,
			func(v error) {
				if skipped < n {
					skipped++
					return
				}
				obs.Error(v)
			},
			obs.Complete,
		))
	})
}
