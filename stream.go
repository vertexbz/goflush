package goflush

// Stream is a lazy, composable stream of values of type T.
// It wraps a subscribe function that wires up the callback chain.
type Stream[T any] struct {
	subscribe func(Observer[T]) Subscription
}

func newStream[T any](subscribe func(Observer[T]) Subscription) *Stream[T] {
	return &Stream[T]{subscribe}
}

// Subscribe is a convenience wrapper: creates an Observer with only Next set.
// Errors and completion signals are ignored.
func (s Stream[T]) Subscribe(f func(T)) Subscription {
	return s.subscribe(&genericObserver[T]{next: f})
}

// SubscribeErr is a convenience wrapper: creates an Observer with only Next & Error set.
// Completion signals are ignored.
func (s Stream[T]) SubscribeErr(f func(T, error)) Subscription {
	return s.subscribe(&genericObserver[T]{
		next: func(t T) {
			f(t, nil)
		},
		err: func(err error) {
			f(*new(T), err)
		},
	})
}

// Observe gives full control over value, error, and completion callbacks.
func (s Stream[T]) Observe(obs Observer[T]) Subscription {
	return s.subscribe(obs)
}
