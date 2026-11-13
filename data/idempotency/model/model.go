// Package model is the storm model of the idempotency table. Include
// &model.IdempotencyRecord{} in storm.Build / storm diff so the table is
// created with your own schema.
package model

import (
	"time"

	"github.com/gsoultan/storm"
)

// IdempotencyRecord remembers the response of one keyed operation.
type IdempotencyRecord struct {
	storm.Model // id uuid, created_at, updated_at

	Key       string     // operation scope + client key
	Response  storm.JSON // the stored response, empty while in progress
	Done      bool
	ExpiresAt time.Time
}

func (r *IdempotencyRecord) Schema(t *storm.Table) {
	t.Col(&r.Key).Size(512)
	t.Col(&r.Done).Default("false")
	t.Unique(&r.Key)
	t.Index(&r.ExpiresAt)
}
