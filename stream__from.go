package goflush

import "iter"

// FromSlice emits every element of items in order on the subscriber's call stack.
// No goroutines are added. Subscribe returns after all items have been emitted.
// Observer.Complete is called after the last item. Observer.Error is never called.
func FromSlice[T any](items []T) *Stream[T] {
	return newStream(func(obs Observer[T]) Subscription {
		for _, item := range items {
			obs.Next(item)
		}
		obs.Complete()
		return noopSubscription{}
	})
}

// FromValues emits the provided values in order, synchronously. Convenience wrapper over FromSlice.
// No goroutines are added. Covers the 1-value, 2-value (and N-value) cases.
func FromValues[T any](values ...T) *Stream[T] {
	return FromSlice(values)
}

// FromGenerator emits values from a Go iterator (iter.Seq[T]).
// No goroutines are added. Subscriber is called synchronously on the Subscribe call stack.
// Observer.Complete is called after the iterator is exhausted. Observer.Error is never called.
func FromGenerator[T any](gen iter.Seq[T]) *Stream[T] {
	return newStream(func(obs Observer[T]) Subscription {
		gen(func(v T) bool {
			obs.Next(v)
			return true
		})
		obs.Complete()
		return noopSubscription{}
	})
}

// FromGeneratorErr emits values from a Go iterator (iter.Seq2[T, error]).
// Values paired with a nil error are forwarded to Observer.Next.
// Values paired with a non-nil error are forwarded to Observer.Error; the value is ignored.
// No goroutines are added. Subscriber is called synchronously on the Subscribe call stack.
// Observer.Complete is called after the iterator is exhausted.
func FromGeneratorErr[T any](gen iter.Seq2[T, error]) *Stream[T] {
	return newStream(func(obs Observer[T]) Subscription {
		gen(func(v T, err error) bool {
			if err != nil {
				obs.Error(err)
			} else {
				obs.Next(v)
			}
			return true
		})
		obs.Complete()
		return noopSubscription{}
	})
}
