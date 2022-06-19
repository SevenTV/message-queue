package messagequeue

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/seventv/message-queue/go/internal/test_utils"
)

func TestMockInit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	inst, err := New(ctx, ConfigMock{})
	test_utils.IsNil(t, err, "Mock instance was made successfully")
	_, ok := inst.(*InstanceMock)
	test_utils.Assert(t, true, ok, "inst is *InstanceMock")
	cancel()
}

func TestMockPublishSubscribe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	inst, err := New(ctx, ConfigMock{})
	test_utils.IsNil(t, err, "Mock instance was made successfully")
	b := []byte{1, 2, 3, 4, 5, 6}
	test_utils.IsNil(t, inst.Publish(ctx, OutgoingMessage{
		Queue: "abc",
		Body:  b,
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
	lCancel()

	select {
	case _, ok := <-msgs:
		test_utils.Assert(t, false, ok, "after the subscription ends clean up the channel")
	case <-time.After(time.Millisecond * 75):
		t.Fatal("the channel was not cleaned up")
	}

	test_utils.IsNil(t, msg.Ack(ctx), "message ack was successful")
	test_utils.IsNil(t, msg.Nack(ctx), "message nack was successful")
	test_utils.IsNil(t, msg.Requeue(ctx), "message requeue was successful")
	test_utils.IsNil(t, msg.Extend(ctx, time.Second*10), "message extend was successful")

	inst.(*InstanceMock).SetConnected(false)

	test_utils.AssertErr(t, ErrNotReady, msg.Ack(ctx), "message ack was not successful")
	test_utils.AssertErr(t, ErrNotReady, msg.Nack(ctx), "message nack was not successful")
	test_utils.AssertErr(t, ErrNotReady, msg.Requeue(ctx), "message requeue was not successful")
	test_utils.AssertErr(t, ErrNotReady, msg.Extend(ctx, time.Second*10), "message extend was not successful")

	test_utils.AssertErr(t, ErrNotReady, inst.Publish(ctx, OutgoingMessage{}), "publish was not successful")

	_, err = inst.Subscribe(lCtx, Subscription{
		Queue: "abc",
	})
	test_utils.AssertErr(t, ErrNotReady, err, "subscribe was not successful")

	inst.(*InstanceMock).SetConnected(true)

	msgs, err = inst.Subscribe(ctx, Subscription{
		Queue: "abc",
	})
	test_utils.IsNil(t, err, "subscribe was successful")

	inst.(*InstanceMock).SetConnected(false)

	select {
	case _, ok := <-msgs:
		test_utils.Assert(t, false, ok, "after a disconnect clean up the channel")
	case <-time.After(time.Millisecond * 75):
		t.Fatal("the channel was not cleaned up")
	}

	inst.(*InstanceMock).SetConnected(true)

	msgs, err = inst.Subscribe(ctx, Subscription{
		Queue: "abc",
	})
	test_utils.IsNil(t, err, "subscribe was successful")

	time.Sleep(time.Millisecond * 100)

	test_utils.IsNil(t, inst.Shutdown(ctx), "shutdown was successful")

	select {
	case _, ok := <-msgs:
		test_utils.Assert(t, false, ok, "after a shutdown clean up the channel")
	case <-time.After(time.Millisecond * 75):
		t.Fatal("the channel was not cleaned up")
	}

	cancel()
}
