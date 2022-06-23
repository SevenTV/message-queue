package messagequeue

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/seventv/message-queue/go/internal/test_utils"
)

func TestRMQInit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	inst, err := New(ctx, ConfigRMQ{
		AmqpURI:              "amqp://user:bitnami@localhost:5672/",
		MaxReconnectAttempts: -1,
	})
	test_utils.IsNil(t, err, "RMQ instance was made successfully")

	_, ok := inst.(*InstanceRMQ)
	test_utils.Assert(t, true, ok, "inst is *InstanceRMQ")

	test_utils.Assert(t, true, inst.Connected(ctx), "RMQ connected successfully")

	cancel()
}

func TestRMQPublishSubscribe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	inst, err := New(ctx, ConfigRMQ{
		AmqpURI:              "amqp://user:bitnami@localhost:5672/",
		MaxReconnectAttempts: -1,
	})
	test_utils.IsNil(t, err, "RMQ instance was made successfully")

	test_utils.Assert(t, true, inst.Connected(ctx), "RMQ connected successfully")

	b := []byte{1, 2, 3, 4, 5, 6}
	test_utils.IsNil(t, inst.Publish(ctx, OutgoingMessage{
		Queue: "abc",
		Body:  b,
		Flags: MessageFlags{
			ContentType:     "binary",
			ID:              "1223",
			ContentEncoding: "xd",
			ReplyTo:         "xdd",
			Timestamp:       time.Now(),
			IsBinary:        true,
		},
		Headers: MessageHeaders{
			"abc": "123",
		},
	}), "Message published successfully")
	lCtx, lCancel := context.WithCancel(ctx)
	msgs, err := inst.Subscribe(lCtx, Subscription{
		Queue: "abc",
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

	test_utils.IsNil(t, msg.Nack(ctx), "message nack was successful")
	test_utils.IsNil(t, msg.Requeue(ctx), "message requeue was successful")
	test_utils.IsNil(t, msg.Ack(ctx), "message ack was successful")
	test_utils.AssertErr(t, ErrUnimplemented, msg.Extend(ctx, time.Second*10), "message extend was not successful")
	lCancel()
	_ = inst.(*InstanceRMQ).conn.Close()

	test_utils.AssertErr(t, ErrNotReady, msg.Ack(ctx), "message ack was not successful")
	test_utils.AssertErr(t, ErrNotReady, msg.Nack(ctx), "message nack was not successful")
	test_utils.AssertErr(t, ErrNotReady, msg.Requeue(ctx), "message requeue was not successful")
	test_utils.AssertErr(t, ErrUnimplemented, msg.Extend(ctx, time.Second*10), "message extend was not successful")

	test_utils.AssertErr(t, ErrNotReady, inst.Publish(ctx, OutgoingMessage{}), "publish was not successful")

	_, err = inst.Subscribe(lCtx, Subscription{
		Queue: "abc",
	})
	test_utils.AssertErr(t, ErrNotReady, err, "subscribe was not successful")

	time.Sleep(time.Millisecond * 75)

	msgs, err = inst.Subscribe(ctx, Subscription{
		Queue: "abc",
	})
	test_utils.IsNil(t, err, "subscribe was successful")

	_ = inst.(*InstanceRMQ).conn.Close()

	timeout := time.After(time.Millisecond * 75)
loop1:
	for {
		select {
		case _, ok := <-msgs:
			if !ok {
				test_utils.Assert(t, false, ok, "after a disconnect clean up the channel")
				break loop1
			}
		case <-timeout:
			t.Fatal("the channel was not cleaned up")
		}
	}

	time.Sleep(time.Millisecond * 75)

	msgs, err = inst.Subscribe(ctx, Subscription{
		Queue: "abc",
	})
	test_utils.IsNil(t, err, "subscribe was successful")

	time.Sleep(time.Millisecond * 75)

	test_utils.IsNil(t, inst.Shutdown(ctx), "shutdown was successful")

	timeout = time.After(time.Millisecond * 75)
loop2:
	for {
		select {
		case _, ok := <-msgs:
			if !ok {
				test_utils.Assert(t, false, ok, "after a shutdown clean up the channel")
				break loop2
			}
		case <-timeout:
			t.Fatal("the channel was not cleaned up")
		}
	}

	cancel()
}

func TestRMQSubscribeLong(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	inst, err := New(ctx, ConfigRMQ{
		AmqpURI:              "amqp://user:bitnami@localhost:5672/",
		MaxReconnectAttempts: -1,
	})
	test_utils.IsNil(t, err, "RMQ instance was made successfully")

	test_utils.Assert(t, true, inst.Connected(ctx), "RMQ connected successfully")

	b := []byte{1, 2, 3, 4, 5, 6}
	test_utils.IsNil(t, inst.Publish(ctx, OutgoingMessage{
		Queue: "abc",
		Body:  b,
		Flags: MessageFlags{
			ContentType:     "binary",
			ID:              "1223",
			ContentEncoding: "xd",
			ReplyTo:         "xdd",
			Timestamp:       time.Now(),
			IsBinary:        true,
		},
		Headers: MessageHeaders{
			"abc": "123",
		},
	}), "Message published successfully")
	lCtx, lCancel := context.WithCancel(ctx)
	msgs, err := inst.Subscribe(lCtx, Subscription{
		Queue: "abc",
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

	time.Sleep(time.Second * 5)

	err = msg.Ack(ctx)
	test_utils.IsNil(t, err, "ack was successful")

	lCancel()
	cancel()
}
