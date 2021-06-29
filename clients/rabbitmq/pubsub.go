package rabbitmq

import (
	"context"
	"time"
)

type publishMsg struct {
	topicId   int
	keySuffix string
	msg       []byte
	expire    time.Duration
	startTime time.Time
	ctx       context.Context
	cancel    context.CancelFunc
	ackErr    error
	// ackChan      chan error
}
