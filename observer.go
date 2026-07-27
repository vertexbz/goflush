package goflush

// Observer is the interface implemented by any subscriber that can receive
// values, errors, and completion signals from a Stream.
type Observer[T any] interface {
	Next(v T)
	Error(err error)
	Complete()
}

// NewObserver creates an Observer from optional callbacks. Nil callbacks are no-ops.
func NewObserver[T any](next func(T), err func(error), complete func()) Observer[T] {
	return &genericObserver[T]{next: next, err: err, complete: complete}
}

// genericObserver is the private concrete implementation of Observer[T].
// Nil callbacks are treated as no-ops.
type genericObserver[T any] struct {
	next     func(T)
	err      func(error)
	complete func()
}

func (o *genericObserver[T]) Next(v T) {
	if o.next != nil {
		o.next(v)
	}
}

func (o *genericObserver[T]) Error(err error) {
	if o.err != nil {
		o.err(err)
	}
}

func (o *genericObserver[T]) Complete() {
	if o.complete != nil {
		o.complete()
	}
}
