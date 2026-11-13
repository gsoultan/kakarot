package transport

import (
	"github.com/gsoultan/kakarot"
	"github.com/gsoultan/kakarot/examples/wallet/domain"
	"github.com/gsoultan/kakarot/examples/wallet/endpoint"
	"github.com/gsoultan/kakarot/queue"
)

// TransferCompleted is the topic the wallet publishes and consumes, typed
// by its payload: binding a handler of another type does not compile.
const TransferCompleted queue.Topic[domain.TransferDone] = "transfer.completed"

// Queue binds the wallet's message handlers to their topics on c.
func Queue(c *queue.Consumer, eps endpoint.Wallet, mws ...kakarot.Middleware) {
	queue.Handle(c, TransferCompleted, "AuditTransfer", eps.AuditTransfer, mws...)
}
