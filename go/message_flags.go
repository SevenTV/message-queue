package messagequeue

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/rabbitmq/amqp091-go"
)

type RMQDeliveryMode uint8

const (
	RMQDeliveryModeTransient  RMQDeliveryMode = RMQDeliveryMode(amqp091.Transient)
	RMQDeliveryModePersistent RMQDeliveryMode = RMQDeliveryMode(amqp091.Persistent)
)

type MessageFlags struct {
	ID              string
	ContentType     string
	ContentEncoding string
	ReplyTo         string
	Timestamp       time.Time
	IsBinary        bool

	RMQ MessageFlagsRMQ
	SQS MessageFlagsSQS
}

type MessageFlagsSQS struct {
	DelaySeconds            int32
	MessageDeduplicationId  *string
	MessageGroupId          *string
	MessageSystemAttributes map[string]types.MessageSystemAttributeValue
}

type MessageFlagsRMQ struct {
	Exchange      string // The RMQ exchange
	Mandatory     bool
	Immediate     bool
	DeliveryMode  RMQDeliveryMode // Transient (0 or 1) or Persistent (2)
	Priority      uint8           // 0 to 9
	CorrelationId string          // correlation identifier
	Expiration    string          // message expiration spec
	Type          string          // message type name
	UserId        string          // creating user id - ex: "guest"
	AppId         string          // creating application id
}
