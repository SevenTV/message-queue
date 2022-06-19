package messagequeue

import (
	"context"
	"sync"
	"time"
)

type InstanceMock struct {
	cfg ConfigMock

	mtx  sync.Mutex
	msgs map[string][]*IncomingMessage

	once     sync.Once
	shutdown chan struct{}

	shutdownMtx sync.Mutex
	stopped     bool
	connected   bool
}

func NewMock(ctx context.Context, cfg ConfigMock) (Instance, error) {
	return &InstanceMock{
		cfg:       cfg,
		msgs:      map[string][]*IncomingMessage{},
		connected: true,
		shutdown:  make(chan struct{}),
	}, nil
}

func (i *InstanceMock) Error() error {
	return nil
}

func (i *InstanceMock) SetConnected(connected bool) {
	i.shutdownMtx.Lock()
	defer i.shutdownMtx.Unlock()

	i.connected = connected
}

func (i *InstanceMock) Connected(ctx context.Context) bool {
	i.shutdownMtx.Lock()
	defer i.shutdownMtx.Unlock()

	return !i.stopped && i.connected
}

func (i *InstanceMock) Subscribe(ctx context.Context, sub Subscription) (<-chan *IncomingMessage, error) {
	if !i.Connected(ctx) {
		return nil, ErrNotReady
	}

	msgQueue := make(chan *IncomingMessage, sub.BufferSize)

	go func() {
		defer close(msgQueue)

		for {
			select {
			case <-ctx.Done():
				return
			case <-i.shutdown:
				return
			case <-time.After(time.Millisecond * 50):
				if !i.Connected(ctx) {
					return
				}

				i.mtx.Lock()
				if len(i.msgs[sub.Queue]) > 0 {
					msg := i.msgs[sub.Queue][0]
					i.msgs[sub.Queue] = i.msgs[sub.Queue][1:]
					i.mtx.Unlock()

					msgQueue <- msg
				} else {
					i.mtx.Unlock()
				}
			}
		}
	}()

	return msgQueue, nil
}

func (i *InstanceMock) Publish(ctx context.Context, msg OutgoingMessage) error {
	if !i.Connected(ctx) {
		return ErrNotReady
	}

	i.mtx.Lock()
	defer i.mtx.Unlock()

	if msg.Flags.Timestamp.IsZero() {
		msg.Flags.Timestamp = time.Now()
	}

	i.msgs[msg.Queue] = append(i.msgs[msg.Queue], &IncomingMessage{
		inst:    i,
		queue:   msg.Queue,
		headers: msg.Headers,
		body:    msg.Body,
		flags:   msg.Flags,
		raw:     nil,
		err:     nil,
	})

	return nil
}

func (i *InstanceMock) Shutdown(ctx context.Context) error {
	i.shutdownMtx.Lock()
	defer i.shutdownMtx.Unlock()

	i.once.Do(func() {
		i.stopped = true
		close(i.shutdown)
	})

	return nil
}

func (i *InstanceMock) ack(ctx context.Context, msg *IncomingMessage) error {
	if !i.Connected(ctx) {
		return ErrNotReady
	}

	return nil
}

func (i *InstanceMock) nack(ctx context.Context, msg *IncomingMessage) error {
	if !i.Connected(ctx) {
		return ErrNotReady
	}

	return nil
}

func (i *InstanceMock) requeue(ctx context.Context, msg *IncomingMessage) error {
	if !i.Connected(ctx) {
		return ErrNotReady
	}

	return nil
}

func (i *InstanceMock) extend(ctx context.Context, msg *IncomingMessage, duration time.Duration) error {
	if !i.Connected(ctx) {
		return ErrNotReady
	}

	return nil
}
