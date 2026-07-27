package goflush

import (
	"context"
	"sync"
)

// Subscription represents an active subscription that can be cancelled.
type Subscription interface {
	Close()
}

type noopSubscription struct{}

func (noopSubscription) Close() {}

type cancelSubscription struct {
	cancel context.CancelFunc
}

func (c *cancelSubscription) Close() { c.cancel() }

type seriesSubscription struct {
	mu      *sync.Mutex
	current *Subscription
	once    sync.Once
}

func (s *seriesSubscription) Close() {
	s.once.Do(func() {
		s.mu.Lock()
		sub := *s.current
		s.mu.Unlock()
		if sub != nil {
			sub.Close()
		}
	})
}

type listSubscription struct {
	subs []Subscription
}

func (c *listSubscription) Close() {
	for _, s := range c.subs {
		s.Close()
	}
}

// sourceSubscription removes itself from the shared observer list when closed.
type sourceSubscription[T any] struct {
	obs    Observer[T]
	shared *sourceShared[T]
}

func (s *sourceSubscription[T]) Close() {
	s.shared.remove(s.obs)
}
