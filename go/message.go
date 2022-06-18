package messagequeue

import (
	"context"
	"time"
)

type OutgoingMessage struct {
	Queue   string
	Headers MessageHeaders
	Flags   MessageFlags
	Body    []byte
}

type IncomingMessage struct {
	inst    Instance
	id      string
	queue   string
	headers MessageHeaders
	body    []byte
	flags   MessageFlags
	raw     any
	err     error
}

func (i *IncomingMessage) Error() error {
	return i.err
}

func (i *IncomingMessage) Ack(ctx context.Context) error {
	if i.err != nil {
		return i.err
	}

	return i.inst.ack(ctx, i)
}

func (i *IncomingMessage) Nack(ctx context.Context) error {
	if i.err != nil {
		return i.err
	}

	return i.inst.nack(ctx, i)
}

func (i *IncomingMessage) Requeue(ctx context.Context) error {
	if i.err != nil {
		return i.err
	}

	return i.inst.requeue(ctx, i)
}

func (i *IncomingMessage) Extend(ctx context.Context, duration time.Duration) error {
	if i.err != nil {
		return i.err
	}

	return i.inst.extend(ctx, i, duration)
}

func (i *IncomingMessage) Queue() string {
	return i.queue
}

func (i *IncomingMessage) Headers() MessageHeaders {
	return i.headers
}

func (i *IncomingMessage) Body() []byte {
	return i.body
}

func (i *IncomingMessage) ID() string {
	return i.id
}
