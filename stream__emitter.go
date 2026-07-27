package goflush

import (
	"sync"
)

// Emitter is a push-based event emitter (Subject pattern). No goroutines added.
// Subscriber callbacks are called on whichever goroutine calls Emit/EmitError/Close.
type Emitter[T any] struct {
	mu   sync.Mutex
	subs []*emitterSub[T]
	done bool
}

type emitterSub[T any] struct {
	obs    Observer[T]
	closed bool
}

// NewEmitter creates a new imperative emitter. No goroutines are added.
func NewEmitter[T any]() *Emitter[T] {
	return &Emitter[T]{}
}

// Emit broadcasts v to all subscribers' Next callbacks. No-op if the emitter is closed.
// Called on the caller's goroutine.
func (e *Emitter[T]) Emit(v T) {
	e.mu.Lock()
	if e.done {
		e.mu.Unlock()
		return
	}
	subs := e.activeSubs()
	e.mu.Unlock()
	for _, sub := range subs {
		sub.obs.Next(v)
	}
}

// EmitError broadcasts err to all subscribers' Error callbacks. No-op if closed.
// Subscribers with nil Error callbacks silently ignore the error.
func (e *Emitter[T]) EmitError(err error) {
	e.mu.Lock()
	if e.done {
		e.mu.Unlock()
		return
	}
	subs := e.activeSubs()
	e.mu.Unlock()
	for _, sub := range subs {
		sub.obs.Error(err)
	}
}

// Close signals completion and removes all subscribers. Subsequent calls are no-ops.
func (e *Emitter[T]) Close() {
	e.mu.Lock()
	if e.done {
		e.mu.Unlock()
		return
	}
	e.done = true
	subs := e.activeSubs()
	e.subs = nil
	e.mu.Unlock()
	for _, sub := range subs {
		sub.obs.Complete()
	}
}

// activeSubs returns a copy of the active (non-closed) subscribers. Must be called with mu held.
func (e *Emitter[T]) activeSubs() []*emitterSub[T] {
	result := make([]*emitterSub[T], 0, len(e.subs))
	for _, s := range e.subs {
		if !s.closed {
			result = append(result, s)
		}
	}
	return result
}

// Stream returns a *Stream[T] backed by this emitter.
// Each call to Subscribe registers a new observer.
func (e *Emitter[T]) Stream() *Stream[T] {
	return newStream(func(obs Observer[T]) Subscription {
		sub := &emitterSub[T]{obs: obs}
		e.mu.Lock()
		if e.done {
			e.mu.Unlock()
			obs.Complete()
			return noopSubscription{}
		}
		e.subs = append(e.subs, sub)
		e.mu.Unlock()
		return &emitterSubscription[T]{emitter: e, sub: sub}
	})
}

type emitterSubscription[T any] struct {
	emitter *Emitter[T]
	sub     *emitterSub[T]
	once    sync.Once
}

func (es *emitterSubscription[T]) Close() {
	es.once.Do(func() {
		es.emitter.mu.Lock()
		es.sub.closed = true
		es.emitter.mu.Unlock()
	})
}
