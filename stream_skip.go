package goflush

// Skip returns a Stream that ignores the first n values, then forwards the rest.
// Only values count toward skipping; errors are always forwarded immediately.
// No goroutines added.
func (s Stream[T]) Skip(n int) *Stream[T] {
	if n <= 0 {
		return &s
	}

	return newStream(func(obs Observer[T]) Subscription {
		skipped := 0
		return s.Observe(NewObserver[T](
			func(v T) {
				if skipped < n {
					skipped++
					return
				}
				obs.Next(v)
			},
			obs.Error,
			obs.Complete,
		))
	})
}
