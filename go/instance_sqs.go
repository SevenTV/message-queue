package messagequeue

import (
	"context"
	"fmt"
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

func (i *InstanceSQS) Connected(ctx context.Context) bool {
	i.mtx.Lock()
	defer i.mtx.Unlock()

	return i.stopped
}

func (i *InstanceSQS) Subscribe(ctx context.Context, sub Subscription) (<-chan *IncomingMessage, error) {
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
				headers := map[string]string{}
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

				msgQueue <- &IncomingMessage{
					id:      *v.MessageId,
					inst:    i,
					queue:   *qurl.QueueUrl,
					headers: headers,
					body:    []byte(*v.Body),
					flags:   MessageFlags{},
					raw:     v,
				}
			}

		}
	}()

	return msgQueue, nil
}

func (i *InstanceSQS) Publish(ctx context.Context, msg OutgoingMessage) error {
	if i.stopped {
		return ErrNotReady
	}

	qurl, err := i.client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(msg.Queue),
	})
	if err != nil {
		return err
	}

	msg.Headers.SetContentEncoding(msg.Flags.ContentEncoding)
	msg.Headers.SetContentType(msg.Flags.ContentType)
	msg.Headers.SetTimestamp(msg.Flags.Timestamp)
	msg.Headers.SetReplyTo(msg.Flags.ReplyTo)
	msg.Headers.SetID(msg.Flags.ID)

	headers := map[string]types.MessageAttributeValue{}
	for k, v := range msg.Headers {
		headers[k] = types.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(v),
		}
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

func (i *InstanceSQS) requeue(ctx context.Context, msg *IncomingMessage) error {
	delivery, ok := msg.raw.(types.Message)
	if !ok {
		return ErrUnknownMessageType
	}

	_, err := i.client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          &msg.queue,
		ReceiptHandle:     delivery.ReceiptHandle,
		VisibilityTimeout: 0,
	})

	return err
}

func (i *InstanceSQS) extend(ctx context.Context, msg *IncomingMessage, duration time.Duration) error {
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
