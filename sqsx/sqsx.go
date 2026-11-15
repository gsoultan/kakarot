// Package sqsx runs kakarot queue consumers and the outbox relay on Amazon
// SQS. A Broker is a queue.Subscriber and an outbox.QueuePublisher.
//
//	cfg, _ := config.LoadDefaultConfig(ctx)
//	broker := &sqsx.Broker{Client: sqs.NewFromConfig(cfg), Queues: map[string]string{
//		"order.placed": "https://sqs.eu-west-1.amazonaws.com/123456789012/billing-order-placed",
//	}}
//
//	consumer := &queue.Consumer{Service: "billing", Source: broker}
//	relay := &outbox.Relay{DB: db, Publisher: outbox.ToQueue(broker)}
//
// Each topic maps to one queue. A queue hands a message to one consumer, so
// give every service that handles a topic its own queue and fan out to them
// with an SNS topic. Delivery is at least once. A message that keeps
// failing moves to the dead-letter queue of the queue's redrive policy.
package sqsx

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/gsoultan/kakarot/queue"
)

const (
	idAttribute  = "Kakarot-Id"
	keyAttribute = "Kakarot-Key"
)

// API is the part of the SQS client a Broker uses. *sqs.Client implements
// it.
type API interface {
	SendMessage(ctx context.Context, in *sqs.SendMessageInput, opts ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, opts ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, in *sqs.DeleteMessageInput, opts ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	ChangeMessageVisibility(ctx context.Context, in *sqs.ChangeMessageVisibilityInput, opts ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
}

// Broker publishes to and consumes from SQS queues.
type Broker struct {
	Client API
	// Queues maps a topic to its queue URL.
	Queues map[string]string
	// Backoff is how long a failed message stays hidden before SQS delivers
	// it again. The default is 5 seconds.
	Backoff time.Duration
	// Wait is the long-poll time of one receive call, at most 20 seconds,
	// which is the default.
	Wait   time.Duration
	Logger *slog.Logger
}

func (b *Broker) queue(topic string) (string, error) {
	if b.Client == nil {
		return "", errors.New("sqsx: Broker.Client is nil")
	}
	url := b.Queues[topic]
	if url == "" {
		return "", errors.New("sqsx: no queue for topic " + topic + " in Broker.Queues")
	}
	return url, nil
}

// Publish sends m to its topic's queue. Headers travel as message
// attributes, of which SQS allows ten; a Broker uses up to two of them. On a
// FIFO queue the key is the message group, which keeps one key's messages
// in order, and the ID deduplicates a repeated publish.
func (b *Broker) Publish(ctx context.Context, m queue.Message) error {
	url, err := b.queue(m.Topic)
	if err != nil {
		return err
	}
	in := &sqs.SendMessageInput{QueueUrl: aws.String(url), MessageBody: aws.String(string(m.Payload)), MessageAttributes: map[string]types.MessageAttributeValue{}}
	set := func(k, v string) {
		if v != "" {
			in.MessageAttributes[k] = types.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(v)}
		}
	}
	for k, v := range m.Headers {
		set(k, v)
	}
	set(idAttribute, m.ID)
	set(keyAttribute, m.Key)
	if strings.HasSuffix(url, ".fifo") {
		group := m.Key
		if group == "" {
			group = m.Topic
		}
		in.MessageGroupId = aws.String(group)
		if m.ID != "" {
			in.MessageDeduplicationId = aws.String(m.ID)
		}
	}
	_, err = b.Client.SendMessage(ctx, in)
	return err
}

// Subscribe delivers messages of topic's queue to h until ctx is cancelled,
// then returns nil. A handled message is deleted; a failed one becomes
// visible again after Backoff.
func (b *Broker) Subscribe(ctx context.Context, topic string, h queue.Handler) error {
	url, err := b.queue(topic)
	if err != nil {
		return err
	}
	backoff, wait := b.Backoff, b.Wait
	if backoff <= 0 {
		backoff = 5 * time.Second
	}
	if wait <= 0 || wait > 20*time.Second {
		wait = 20 * time.Second
	}
	for ctx.Err() == nil {
		out, err := b.Client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:                    aws.String(url),
			MaxNumberOfMessages:         10,
			WaitTimeSeconds:             int32(wait / time.Second),
			MessageAttributeNames:       []string{"All"},
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameApproximateReceiveCount},
		})
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			b.log().ErrorContext(ctx, "sqs receive failed", "topic", topic, "err", err)
			select { // do not spin on a queue that is unreachable
			case <-ctx.Done():
			case <-time.After(backoff):
			}
			continue
		}
		for _, raw := range out.Messages {
			if ctx.Err() != nil {
				break // not handled: visible again after the queue's own timeout
			}
			m := message(topic, raw)
			if err := h(ctx, m); err != nil {
				_, verr := b.Client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
					QueueUrl: aws.String(url), ReceiptHandle: raw.ReceiptHandle,
					VisibilityTimeout: int32((backoff + time.Second - 1) / time.Second),
				})
				if verr != nil && ctx.Err() == nil {
					b.log().ErrorContext(ctx, "sqs change visibility failed", "topic", topic, "id", m.ID, "err", verr)
				}
				continue
			}
			// A handler that finished is acknowledged even when ctx ended meanwhile.
			if _, derr := b.Client.DeleteMessage(context.WithoutCancel(ctx), &sqs.DeleteMessageInput{QueueUrl: aws.String(url), ReceiptHandle: raw.ReceiptHandle}); derr != nil {
				b.log().ErrorContext(ctx, "sqs delete failed, the message will be delivered again", "topic", topic, "id", m.ID, "err", derr)
			}
		}
	}
	return nil
}

func message(topic string, raw types.Message) queue.Message {
	m := queue.Message{ID: aws.ToString(raw.MessageId), Topic: topic, Payload: []byte(aws.ToString(raw.Body)), Headers: map[string]string{}}
	for k, v := range raw.MessageAttributes {
		switch k {
		case idAttribute:
			m.ID = aws.ToString(v.StringValue)
		case keyAttribute:
			m.Key = aws.ToString(v.StringValue)
		default:
			m.Headers[k] = aws.ToString(v.StringValue)
		}
	}
	if n, err := strconv.Atoi(raw.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)]); err == nil && n > 1 {
		m.Attempts = int32(n - 1)
	}
	return m
}

func (b *Broker) log() *slog.Logger {
	if b.Logger != nil {
		return b.Logger
	}
	return slog.Default()
}
