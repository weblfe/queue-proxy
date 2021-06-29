package rabbitmq

import "errors"

var (
	ErrAck     = errors.New("ack")
	ErrNack    = errors.New("nack")
	ErrFull    = errors.New("full")
	ErrCancel  = errors.New("cancel")
	ErrTimeout = errors.New("timeout")
)