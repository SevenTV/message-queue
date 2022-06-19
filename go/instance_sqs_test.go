package messagequeue

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/seventv/message-queue/go/internal/test_utils"
)

func TestSQSInit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	inst, err := New(ctx, ConfigSQS{
		Region: "us-east-2",
		Credentials: aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
			creds, err := credentials.NewSharedCredentials("", "").GetWithContext(ctx)
			if err != nil {
				return aws.Credentials{}, err
			}
			return aws.Credentials{
				AccessKeyID:     creds.AccessKeyID,
				SecretAccessKey: creds.SecretAccessKey,
				SessionToken:    creds.SessionToken,
			}, nil
		}),
	})
	test_utils.IsNil(t, err, "SQS instance was made successfully")

	_, ok := inst.(*InstanceSQS)
	test_utils.Assert(t, true, ok, "inst is *InstanceSQS")

	test_utils.Assert(t, true, inst.Connected(ctx), "SQS connected successfully")

	cancel()
}

func TestSQSPublishSubscribe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	inst, err := New(ctx, ConfigSQS{
		Region: "us-east-2",
		Credentials: aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
			creds, err := credentials.NewSharedCredentials("", "").GetWithContext(ctx)
			if err != nil {
				return aws.Credentials{}, err
			}
			return aws.Credentials{
				AccessKeyID:     creds.AccessKeyID,
				SecretAccessKey: creds.SecretAccessKey,
				SessionToken:    creds.SessionToken,
			}, nil
		}),
	})
	test_utils.IsNil(t, err, "SQS instance was made successfully")

	b := []byte{1, 2, 3, 4, 5}
	test_utils.IsNil(t, inst.Publish(ctx, OutgoingMessage{
		Queue: "test",
		Body:  b,
		Flags: MessageFlags{
			ContentType:     "binary",
			ID:              "1223",
			ContentEncoding: "xd",
			ReplyTo:         "xdd",
			Timestamp:       time.Now(),
			IsBinary:        true,
			SQS:             MessageFlagsSQS{},
		},
		Headers: MessageHeaders{
			"abc": "123",
		},
	}), "Message published successfully")
	lCtx, lCancel := context.WithCancel(ctx)
	msgs, err := inst.Subscribe(lCtx, Subscription{
		Queue: "test",
		SQS: SubscriptionSQS{
			AttributeNames:        []types.QueueAttributeName{types.QueueAttributeNameAll},
			MessageAttributeNames: []string{"*"},
		},
	})
	test_utils.IsNil(t, err, "Subscribe was successful")
	var msg *IncomingMessage
	select {
	case msg = <-msgs:
		h := sha256.New()
		h.Write(b)
		hash := hex.EncodeToString(h.Sum(nil))
		h.Reset()
		h.Write(msg.body)
		msgHash := hex.EncodeToString(h.Sum(nil))
		test_utils.Assert(t, hash, msgHash, "The message content is the same")
	case <-time.After(time.Millisecond * 100):
		t.Fatal("message was not recieved in time")
	}

	test_utils.IsNil(t, msg.Requeue(ctx), "message requeue was successful")

	select {
	case msg = <-msgs:
		h := sha256.New()
		h.Write(b)
		hash := hex.EncodeToString(h.Sum(nil))
		h.Reset()
		h.Write(msg.body)
		msgHash := hex.EncodeToString(h.Sum(nil))
		test_utils.Assert(t, hash, msgHash, "The message content is the same")
	case <-time.After(time.Millisecond * 100):
		t.Fatal("message was not recieved in time")
	}

	test_utils.IsNil(t, msg.Extend(ctx, time.Second*10), "message extend was not successful")

	test_utils.IsNil(t, msg.Ack(ctx), "message ack was successful")

	test_utils.IsNil(t, inst.Publish(ctx, OutgoingMessage{
		Queue: "test",
		Body:  b,
		Flags: MessageFlags{
			ContentType:     "binary",
			ID:              "1223",
			ContentEncoding: "xd",
			ReplyTo:         "xdd",
			IsBinary:        true,
			Timestamp:       time.Now(),
			SQS:             MessageFlagsSQS{},
		},
		Headers: MessageHeaders{
			"abc": "123",
		},
	}), "Message published successfully")

	select {
	case msg = <-msgs:
		h := sha256.New()
		h.Write(b)
		hash := hex.EncodeToString(h.Sum(nil))
		h.Reset()
		h.Write(msg.body)
		msgHash := hex.EncodeToString(h.Sum(nil))
		test_utils.Assert(t, hash, msgHash, "The message content is the same")
	case <-time.After(time.Millisecond * 100):
		t.Fatal("message was not recieved in time")
	}

	test_utils.IsNil(t, msg.Nack(ctx), "message nack was successful")

	lCancel()

	time.Sleep(time.Millisecond * 75)

	msgs, err = inst.Subscribe(ctx, Subscription{
		Queue: "test",
	})
	test_utils.IsNil(t, err, "subscribe was successful")

	time.Sleep(time.Millisecond * 75)

	test_utils.IsNil(t, inst.Shutdown(ctx), "shutdown was successful")

	timeout := time.After(time.Millisecond * 75)
loop:
	for {
		select {
		case _, ok := <-msgs:
			if !ok {
				test_utils.Assert(t, false, ok, "after a shutdown clean up the channel")
				break loop
			}
		case <-timeout:
			t.Fatal("the channel was not cleaned up")
		}
	}

	cancel()
}
