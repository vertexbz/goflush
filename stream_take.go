package goflush

// Take returns a Stream that emits at most n values, then calls Complete and closes.
// Errors are always forwarded. No goroutines added.
func (s Stream[T]) Take(n int) *Stream[T] {
	if n <= 0 {
		return &s
	}

	return newStream(func(obs Observer[T]) Subscription {
		count := 0
		done := false
		// Use a holder so the closure can reference the subscription even for
		// synchronous sources where Next fires before SubscribeObserver returns.
		type holder struct{ sub Subscription }
		h := &holder{}
		h.sub = s.Observe(NewObserver[T](
			func(v T) {
				if done || count >= n {
					return
				}
				count++
				obs.Next(v)
				if count >= n && !done {
					done = true
					obs.Complete()
					if h.sub != nil {
						h.sub.Close()
					}
				}
			},
			obs.Error,
			obs.Complete,
		))
		return h.sub
	})
}
