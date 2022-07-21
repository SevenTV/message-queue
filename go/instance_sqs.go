package messagequeue

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type InstanceSQS struct {
	cfg    ConfigSQS
	client *sqs.Client

	once     sync.Once
	shutdown chan struct{}

	mtx     sync.Mutex
	stopped bool
}

func NewSQS(ctx context.Context, cfg ConfigSQS) (Instance, error) {
	return &InstanceSQS{
		cfg:      cfg,
		shutdown: make(chan struct{}),
		client: sqs.New(sqs.Options{
			Credentials:      cfg.Credentials,
			Region:           cfg.Region,
			RetryMaxAttempts: cfg.RetryMaxAttempts,
		}),
	}, nil
}

func (i *InstanceSQS) Error() error {
	return nil
}

func (i *InstanceSQS) Connected(ctx context.Context) bool {
	i.mtx.Lock()
	defer i.mtx.Unlock()

	return !i.stopped
}

func (i *InstanceSQS) Subscribe(ctx context.Context, sub Subscription) (<-chan *IncomingMessage, error) {
	if !i.Connected(ctx) {
		return nil, ErrNotReady
	}

	msgQueue := make(chan *IncomingMessage, sub.BufferSize)

	qurl, err := i.client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(sub.Queue),
	})
	if err != nil {
		return nil, err
	}

	go func() {
		defer close(msgQueue)

		for {
			select {
			case <-ctx.Done():
				return
			case <-i.shutdown:
				return
			default:
			}
			if len(sub.SQS.AttributeNames) == 0 {
				sub.SQS.AttributeNames = append(sub.SQS.AttributeNames, types.QueueAttributeNameAll)
			}
			if len(sub.SQS.MessageAttributeNames) == 0 {
				sub.SQS.MessageAttributeNames = append(sub.SQS.MessageAttributeNames, "*")
			}

			msg, err := i.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
				VisibilityTimeout:     sub.SQS.VisibilityTimeout,
				MaxNumberOfMessages:   sub.SQS.MaxNumberOfMessages,
				WaitTimeSeconds:       sub.SQS.WaitTimeSeconds,
				AttributeNames:        sub.SQS.AttributeNames,
				MessageAttributeNames: sub.SQS.MessageAttributeNames,
				QueueUrl:              qurl.QueueUrl,
			})
			if err != nil {
				msgQueue <- &IncomingMessage{err: err}
				continue
			}

			for _, v := range msg.Messages {
				headers := MessageHeaders{}
				for k, v := range v.MessageAttributes {
					if v.StringValue != nil {
						headers[k] = *v.StringValue
					} else {
						headers[k] = fmt.Sprint(v.BinaryValue)
					}
				}
				for k, v := range v.Attributes {
					headers[k] = v
				}

				body := *v.Body
				if headers.IsBinary() {
					data, err := base64.StdEncoding.DecodeString(body)
					if err != nil {
						msgQueue <- &IncomingMessage{err: err}
						continue
					}
					body = string(data)
				}

				msgQueue <- &IncomingMessage{
					id:      *v.MessageId,
					inst:    i,
					queue:   *qurl.QueueUrl,
					headers: headers,
					body:    []byte(body),
					flags: MessageFlags{
						ID:              headers.ID(),
						ContentType:     headers.ContentType(),
						ContentEncoding: headers.ContentEncoding(),
						ReplyTo:         headers.ReplyTo(),
						Timestamp:       headers.Timestamp(),
						IsBinary:        headers.IsBinary(),
					},
					raw: v,
				}
			}

		}
	}()

	return msgQueue, nil
}

func (i *InstanceSQS) Publish(ctx context.Context, msg OutgoingMessage) error {
	if !i.Connected(ctx) {
		return ErrNotReady
	}

	qurl, err := i.client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(msg.Queue),
	})
	if err != nil {
		return err
	}

	if msg.Flags.Timestamp.IsZero() {
		msg.Flags.Timestamp = time.Now()
	}

	if msg.Headers == nil {
		msg.Headers = MessageHeaders{}
	}

	msg.Headers.SetContentEncoding(msg.Flags.ContentEncoding)
	msg.Headers.SetContentType(msg.Flags.ContentType)
	msg.Headers.SetTimestamp(msg.Flags.Timestamp)
	msg.Headers.SetIsBinary(msg.Flags.IsBinary)
	msg.Headers.SetReplyTo(msg.Flags.ReplyTo)
	msg.Headers.SetID(msg.Flags.ID)
	msg.Headers.Harden()

	headers := map[string]types.MessageAttributeValue{}
	for k, v := range msg.Headers {
		headers[k] = types.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(v),
		}
	}

	// these queues require a few extra paramaters
	if strings.HasSuffix(msg.Queue, ".fifo") {
		if msg.Flags.SQS.MessageGroupId == nil {
			msg.Flags.SQS.MessageGroupId = aws.String("default")
		}
		if msg.Flags.SQS.MessageDeduplicationId == nil {
			if msg.Flags.ID == "" {
				bytes := make([]byte, 5)
				_, _ = rand.Read(bytes)
				msg.Flags.SQS.MessageDeduplicationId = aws.String(fmt.Sprintf("default-%d-%s", time.Now().UnixNano(), hex.EncodeToString(bytes)))
			} else {
				msg.Flags.SQS.MessageDeduplicationId = aws.String(msg.Flags.ID)
			}
		}
	}

	if msg.Flags.IsBinary {
		msg.Body = []byte(base64.StdEncoding.EncodeToString(msg.Body))
	}

	_, err = i.client.SendMessage(ctx, &sqs.SendMessageInput{
		MessageBody:             aws.String(string(msg.Body)),
		QueueUrl:                qurl.QueueUrl,
		DelaySeconds:            msg.Flags.SQS.DelaySeconds,
		MessageAttributes:       headers,
		MessageDeduplicationId:  msg.Flags.SQS.MessageDeduplicationId,
		MessageGroupId:          msg.Flags.SQS.MessageGroupId,
		MessageSystemAttributes: msg.Flags.SQS.MessageSystemAttributes,
	})

	return err
}

func (i *InstanceSQS) Shutdown(ctx context.Context) error {
	i.once.Do(func() {
		i.mtx.Lock()
		defer i.mtx.Unlock()

		i.stopped = true
		close(i.shutdown)
	})

	return nil
}

func (i *InstanceSQS) ack(ctx context.Context, msg *IncomingMessage) error {
	if !i.Connected(ctx) {
		return ErrNotReady
	}

	delivery, ok := msg.raw.(types.Message)
	if !ok {
		return ErrUnknownMessageType
	}

	_, err := i.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      &msg.queue,
		ReceiptHandle: delivery.ReceiptHandle,
	})

	return err
}

func (i *InstanceSQS) nack(ctx context.Context, msg *IncomingMessage) error {
	return i.ack(ctx, msg)
}

func (i *InstanceSQS) requeue(ctx context.Context, msg *IncomingMessage) error {
	if !i.Connected(ctx) {
		return ErrNotReady
	}

	delivery, ok := msg.raw.(types.Message)
	if !ok {
		return ErrUnknownMessageType
	}

	_, err := i.client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          &msg.queue,
		ReceiptHandle:     delivery.ReceiptHandle,
		VisibilityTimeout: 5,
	})

	return err
}

func (i *InstanceSQS) extend(ctx context.Context, msg *IncomingMessage, duration time.Duration) error {
	if !i.Connected(ctx) {
		return ErrNotReady
	}

	delivery, ok := msg.raw.(types.Message)
	if !ok {
		return ErrUnknownMessageType
	}

	_, err := i.client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          &msg.queue,
		ReceiptHandle:     delivery.ReceiptHandle,
		VisibilityTimeout: int32(duration / time.Second),
	})

	return err
}
