// Package model is the storm model of the transactional outbox table.
// Include &model.OutboxEvent{} in storm.Build / storm diff so the table is
// created with your own schema.
package model

import (
	"time"

	"github.com/gsoultan/storm"
)

// OutboxEvent is one event written in the same transaction as the business
// change, published later by the relay.
type OutboxEvent struct {
	storm.Model // id uuid, created_at, updated_at

	Topic       string
	Key         string
	Payload     storm.JSON
	Attempts    int32
	LastError   string
	PublishedAt *time.Time
}

func (e *OutboxEvent) Schema(t *storm.Table) {
	t.Col(&e.Topic).Size(255)
	t.Col(&e.Key).Size(255)
	t.Col(&e.Attempts).Default("0")
	t.Col(&e.LastError).Default("''")
	t.Index(&e.PublishedAt, &e.CreatedAt)
}
