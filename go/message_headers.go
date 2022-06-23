package messagequeue

import (
	"fmt"
	"strconv"
	"time"
)

type MessageHeaders map[string]string

const (
	MessageHeaderReplyTo         = "MessageQueue.ReplyTo"
	MessageHeaderTimestamp       = "MessageQueue.Timestamp"
	MessageHeaderContentType     = "MessageQueue.ContentType"
	MessageHeaderContentEncoding = "MessageQueue.ContentEncoding"
	MessageHeaderID              = "MessageQueue.ID"
	MessageHeaderIsBinary        = "MessageQueue.IsBinary"
)

func (m MessageHeaders) Harden() {
	for k, v := range m {
		if v == "" {
			delete(m, k)
		}
	}
}

func (m MessageHeaders) ReplyTo() string {
	return m[MessageHeaderReplyTo]
}

func (m MessageHeaders) Timestamp() time.Time {
	t, _ := time.Parse(m[MessageHeaderTimestamp], time.RFC3339)
	return t
}

func (m MessageHeaders) ContentType() string {
	return m[MessageHeaderContentType]
}

func (m MessageHeaders) ContentEncoding() string {
	return m[MessageHeaderContentEncoding]
}

func (m MessageHeaders) ID() string {
	return m[MessageHeaderID]
}

func (m MessageHeaders) IsBinary() bool {
	b, _ := strconv.ParseBool(m[MessageHeaderIsBinary])
	return b
}

func (m MessageHeaders) SetReplyTo(value string) {
	m[MessageHeaderReplyTo] = value
}

func (m MessageHeaders) SetTimestamp(value time.Time) {
	m[MessageHeaderTimestamp] = value.Format(time.RFC3339)
}

func (m MessageHeaders) SetContentType(value string) {
	m[MessageHeaderContentType] = value
}

func (m MessageHeaders) SetContentEncoding(value string) {
	m[MessageHeaderContentEncoding] = value
}

func (m MessageHeaders) SetID(value string) {
	m[MessageHeaderID] = value
}

func (m MessageHeaders) SetIsBinary(value bool) {
	m[MessageHeaderIsBinary] = fmt.Sprint(value)
}
