package messagequeue

import (
	"context"
	"time"
)

type Instance interface {
	// Subscribe consumes a queue when the context expires all the channels, and memory is cleaned up and the consumer is closed.
	Subscribe(ctx context.Context, sub Subscription) (<-chan *IncomingMessage, error)
	// Publish publishes a message onto the queue, the context is used to do the publish action, if the context expires the action is canceled.
	Publish(ctx context.Context, msg OutgoingMessage) error
	// Shutdown stops all current subscriptions and also closes the connections.
	Shutdown(ctx context.Context) error

	// Connected returns true if the connection is ready
	Connected(ctx context.Context) bool

	// Error is the instance has failed
	Error() error

	// InternalMethods
	ack(ctx context.Context, msg *IncomingMessage) error
	nack(ctx context.Context, msg *IncomingMessage) error
	requeue(ctx context.Context, msg *IncomingMessage) error
	extend(ctx context.Context, msg *IncomingMessage, duration time.Duration) error
}

func New(ctx context.Context, cfg Config) (Instance, error) {
	switch c := cfg.(type) {
	case ConfigMock:
		return NewMock(ctx, c)
	case ConfigRMQ:
		return NewRMQ(ctx, c)
	case ConfigSQS:
		return NewSQS(ctx, c)
	default:
		return nil, ErrUnknownConfigType
	}
}
