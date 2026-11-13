package queue

import (
	"context"
	"sync"
)

// Memory is an in-process broker. Publish calls every handler subscribed to
// the message's topic and returns the first error, so a producer that
// retries (such as the outbox relay) gives at-least-once delivery. Messages
// published to a topic nobody subscribes to are dropped.
type Memory struct {
	mu   sync.RWMutex
	next int
	subs map[string]map[int]Handler
}

// Subscribe implements Subscriber.
func (b *Memory) Subscribe(ctx context.Context, topic string, h Handler) error {
	b.mu.Lock()
	if b.subs == nil {
		b.subs = map[string]map[int]Handler{}
	}
	if b.subs[topic] == nil {
		b.subs[topic] = map[int]Handler{}
	}
	id := b.next
	b.next++
	b.subs[topic][id] = h
	b.mu.Unlock()

	<-ctx.Done()
	b.mu.Lock()
	delete(b.subs[topic], id)
	b.mu.Unlock()
	return nil
}

// Publish delivers m to the topic's subscribers and waits for them.
func (b *Memory) Publish(ctx context.Context, m Message) error {
	b.mu.RLock()
	hs := make([]Handler, 0, len(b.subs[m.Topic]))
	for _, h := range b.subs[m.Topic] {
		hs = append(hs, h)
	}
	b.mu.RUnlock()
	for _, h := range hs {
		if err := h(ctx, m); err != nil {
			return err
		}
	}
	return nil
}

// Subscribers reports how many handlers are subscribed to topic.
func (b *Memory) Subscribers(topic string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subs[topic])
}
