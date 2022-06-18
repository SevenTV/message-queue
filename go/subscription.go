package messagequeue

import (
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/rabbitmq/amqp091-go"
)

type Subscription struct {
	Queue      string
	BufferSize uint
	RMQ        SubscriptionRMQ
	SQS        SubscriptionSQS
}

type SubscriptionRMQ struct {
	Consumer  string
	AutoAck   bool
	Exclusive bool
	NoLocal   bool
	NoWait    bool
	Args      amqp091.Table
}

type SubscriptionSQS struct {
	VisibilityTimeout     int32
	MaxNumberOfMessages   int32
	WaitTimeSeconds       int32
	AttributeNames        []types.QueueAttributeName
	MessageAttributeNames []string
}
