package messagequeue

type Error string

func (e Error) Error() string {
	return string(e)
}

const (
	ErrUnimplemented      Error = "unimplemented"
	ErrUnknownConfigType  Error = "unknown config type"
	ErrUnknownMessageType Error = "unknown message type"
	ErrNotReady           Error = "not ready"
)
