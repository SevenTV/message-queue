package messagequeue

import (
	"context"
	"encoding/base64"
	"fmt"
	"sync"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"go.uber.org/multierr"
)

type InstanceRMQ struct {
	cfg     ConfigRMQ
	conn    *amqp091.Connection
	channel *amqp091.Channel

	ready    chan struct{}
	once     sync.Once
	shutdown chan struct{}
	mtx      sync.Mutex
	err      error
}

func NewRMQ(ctx context.Context, cfg ConfigRMQ) (Instance, error) {
	i := &InstanceRMQ{
		cfg:      cfg,
		shutdown: make(chan struct{}),
		ready:    make(chan struct{}),
	}

	go i.autoReconnect()

	return i, nil
}

func (i *InstanceRMQ) Error() error {
	return i.err
}

func (i *InstanceRMQ) autoReconnect() {
	tick := time.NewTicker(time.Millisecond * 50)
	defer tick.Stop()

	once := sync.Once{}
	failedAttempts := 0

	connect := func() error {
		conn, err := amqp091.Dial(i.cfg.AmqpURI)
		if err != nil {
			return err
		}

		channel, err := conn.Channel()
		if err != nil {
			return multierr.Append(err, conn.Close())
		}

		if i.channel != nil && i.conn != nil {
			_ = i.channel.Close()
			_ = i.conn.Close()
		}

		if i.conn != nil {
			once.Do(func() {
				close(i.ready)
			})
			i.mtx.Lock()
			i.ready = make(chan struct{})
			i.conn = conn
			i.channel = channel
			once = sync.Once{}
			failedAttempts = 0
			i.mtx.Unlock()
		} else {
			i.conn = conn
			i.channel = channel
		}

		return nil
	}

	if err := connect(); err != nil {
		failedAttempts++
		if i.cfg.MaxReconnectAttempts != 0 && failedAttempts > i.cfg.MaxReconnectAttempts {
			i.err = multierr.Append(err, i.Shutdown(context.Background()))
			return
		}
	}

	for {
		select {
		case <-i.shutdown:
			return
		case <-tick.C:
			if i.channel == nil || i.channel.IsClosed() {
				if err := connect(); err != nil {
					failedAttempts++
					if i.cfg.MaxReconnectAttempts != 0 && failedAttempts > i.cfg.MaxReconnectAttempts {
						i.err = multierr.Append(err, i.Shutdown(context.Background()))
						return
					}
				}
			} else {
				once.Do(func() {
					close(i.ready)
				})
			}
		}
	}
}

func (i *InstanceRMQ) Connected(ctx context.Context) bool {
	i.mtx.Lock()
	defer i.mtx.Unlock()

	select {
	case <-ctx.Done():
	case <-i.ready:
	}

	return i.channel != nil && !i.channel.IsClosed()
}

func (i *InstanceRMQ) Subscribe(ctx context.Context, sub Subscription) (<-chan *IncomingMessage, error) {
	if !i.Connected(ctx) {
		return nil, ErrNotReady
	}

	ch, err := i.conn.Channel()
	if err != nil {
		return nil, err
	}

	delivery, err := ch.Consume(sub.Queue, sub.RMQ.Consumer, sub.RMQ.AutoAck, sub.RMQ.Exclusive, sub.RMQ.Exclusive, sub.RMQ.NoWait, sub.RMQ.Args)
	if err != nil {
		return nil, err
	}

	msgQueue := make(chan *IncomingMessage, sub.BufferSize)

	tick := time.NewTicker(time.Millisecond * 50)
	go func() {
		defer ch.Close()
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-i.shutdown:
				return
			case <-tick.C:
				if ch.IsClosed() {
					return
				}
			}
		}
	}()

	go func() {
		defer ch.Close()
		defer close(msgQueue)
		defer panic(1)

		for msg := range delivery {
			headers := MessageHeaders{}
			for k, v := range msg.Headers {
				headers[k] = fmt.Sprint(v)
			}

			body := string(msg.Body)
			if headers.IsBinary() {
				data, err := base64.StdEncoding.DecodeString(body)
				if err != nil {
					msgQueue <- &IncomingMessage{err: err}
					continue
				}

				body = string(data)
			}

			msgQueue <- &IncomingMessage{
				inst:    i,
				queue:   sub.Queue,
				headers: headers,
				body:    []byte(body),
				flags: MessageFlags{
					ID:              headers.ID(),
					ContentType:     headers.ContentType(),
					ContentEncoding: headers.ContentEncoding(),
					ReplyTo:         headers.ReplyTo(),
					Timestamp:       headers.Timestamp(),
					IsBinary:        headers.IsBinary(),
					RMQ: MessageFlagsRMQ{
						Exchange:      msg.Exchange,
						DeliveryMode:  RMQDeliveryMode(msg.DeliveryMode),
						Priority:      msg.Priority,
						CorrelationId: msg.CorrelationId,
						Expiration:    msg.Expiration,
						Type:          msg.Type,
						UserId:        msg.UserId,
						AppId:         msg.AppId,
					},
				},
				raw: &msg,
			}
		}
	}()

	return msgQueue, nil
}

func (i *InstanceRMQ) Publish(ctx context.Context, msg OutgoingMessage) error {
	if !i.Connected(ctx) {
		return ErrNotReady
	}

	if msg.Headers == nil {
		msg.Headers = MessageHeaders{}
	}

	if msg.Flags.Timestamp.IsZero() {
		msg.Flags.Timestamp = time.Now()
	}

	msg.Headers.SetContentEncoding(msg.Flags.ContentEncoding)
	msg.Headers.SetContentType(msg.Flags.ContentType)
	msg.Headers.SetTimestamp(msg.Flags.Timestamp)
	msg.Headers.SetIsBinary(msg.Flags.IsBinary)
	msg.Headers.SetReplyTo(msg.Flags.ReplyTo)
	msg.Headers.SetID(msg.Flags.ID)
	msg.Headers.Harden()

	if msg.Flags.IsBinary {
		msg.Body = []byte(base64.StdEncoding.EncodeToString(msg.Body))
	}

	headers := amqp091.Table{}
	for k, v := range msg.Headers {
		headers[k] = v
	}

	return i.channel.Publish(msg.Flags.RMQ.Exchange, msg.Queue, msg.Flags.RMQ.Mandatory, msg.Flags.RMQ.Immediate, amqp091.Publishing{
		Headers:         headers,
		ContentType:     msg.Flags.ContentType,
		ContentEncoding: msg.Flags.ContentEncoding,
		DeliveryMode:    uint8(msg.Flags.RMQ.DeliveryMode),
		Priority:        msg.Flags.RMQ.Priority,
		CorrelationId:   msg.Flags.RMQ.CorrelationId,
		ReplyTo:         msg.Flags.ReplyTo,
		Expiration:      msg.Flags.RMQ.Expiration,
		MessageId:       msg.Flags.ID,
		Timestamp:       msg.Flags.Timestamp,
		Type:            msg.Flags.RMQ.Type,
		UserId:          msg.Flags.RMQ.UserId,
		AppId:           msg.Flags.RMQ.AppId,
		Body:            msg.Body,
	})
}

func (i *InstanceRMQ) Shutdown(ctx context.Context) error {
	var err error

	i.once.Do(func() {
		if i.channel != nil && i.conn != nil {
			err = multierr.Append(i.channel.Close(), i.conn.Close())
		}
		close(i.shutdown)
	})

	return err
}

func (i *InstanceRMQ) ack(ctx context.Context, msg *IncomingMessage) error {
	if !i.Connected(ctx) {
		return ErrNotReady
	}

	delivery, ok := msg.raw.(*amqp091.Delivery)
	if !ok {
		return ErrUnknownMessageType
	}

	return delivery.Ack(false)
}

func (i *InstanceRMQ) nack(ctx context.Context, msg *IncomingMessage) error {
	if !i.Connected(ctx) {
		return ErrNotReady
	}

	delivery, ok := msg.raw.(*amqp091.Delivery)
	if !ok {
		return ErrUnknownMessageType
	}

	return delivery.Nack(false, false)
}

func (i *InstanceRMQ) requeue(ctx context.Context, msg *IncomingMessage) error {
	if !i.Connected(ctx) {
		return ErrNotReady
	}

	delivery, ok := msg.raw.(*amqp091.Delivery)
	if !ok {
		return ErrUnknownMessageType
	}

	return delivery.Reject(true)
}

func (i *InstanceRMQ) extend(ctx context.Context, msg *IncomingMessage, duration time.Duration) error {
	return ErrUnimplemented
}
