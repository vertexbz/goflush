package goflush

// Buffer accumulates n values then emits them as a slice.
// Errors are forwarded immediately (do not wait for buffer to fill).
// On Complete, any partial buffer is flushed before calling downstream Complete.
// No goroutines added.
func Buffer[T any](s *Stream[T], n int) *Stream[[]T] {
	return newStream(func(obs Observer[[]T]) Subscription {
		buf := make([]T, 0, n)
		return s.Observe(NewObserver[T](
			func(v T) {
				buf = append(buf, v)
				if len(buf) >= n {
					out := make([]T, len(buf))
					copy(out, buf)
					buf = buf[:0]
					obs.Next(out)
				}
			},
			obs.Error,
			func() {
				if len(buf) > 0 {
					out := make([]T, len(buf))
					copy(out, buf)
					buf = buf[:0]
					obs.Next(out)
				}
				obs.Complete()
			},
		))
	})
}
