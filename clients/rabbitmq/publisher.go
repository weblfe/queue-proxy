package rabbitmq

import (
	"fmt"
	"github.com/streadway/amqp"
	"sync"
)

type (
	Publisher interface {
		Send(MessageWrapper) error
		Close() error
	}

	SimpleQueuePublisher struct {
		client      *Client
		destructor  sync.Once
		constructor sync.Once
		queueParams *QueueParams
		queue       *amqp.Queue
	}
)

func NewSimpleQueuePublisher(client *Client, queueParams *QueueParams) *SimpleQueuePublisher {
	return &SimpleQueuePublisher{
		client:      client,
		destructor:  sync.Once{},
		constructor: sync.Once{},
		queueParams: queueParams,
	}
}

func (Publisher *SimpleQueuePublisher) GetBroker() *Broker {
	return Publisher.client.GetBroker()
}

// 发送消息
func (Publisher *SimpleQueuePublisher) Send(message MessageWrapper) error {
	if Publisher.client.Closed() {
		return fmt.Errorf("[Publisher] Client Connection Closed")
	}
	// 队列是否初始化
	if err := Publisher.initQueue(); err != nil {
		return err
	}
	if message == nil {
		return fmt.Errorf("[Publisher] Client message is Nil")
	}
	var (
		row   = message.GetRowMessage()
		queue = Publisher.GetQueueName()
	)
	return Publisher.client.SendToQueue(queue, Publisher.queue.Name, row)
}

// 发送消息
func (Publisher *SimpleQueuePublisher) Publish(message MessageParams) error {
	if Publisher.client.Closed() {
		return fmt.Errorf("[Publisher] Client Connection Closed")
	}
	return Publisher.client.Send(message)
}

// GetQueueName 获取队列名
func (Publisher *SimpleQueuePublisher) GetQueueName() string {
	if Publisher.queueParams.Name == "" {
		return defaultQueueName
	}
	return Publisher.queueParams.Name
}

// initQueue 初始化 队列
func (Publisher *SimpleQueuePublisher) initQueue() error {
	var (
		err     error
		queue   amqp.Queue
		channel = Publisher.GetChannel()
	)
	Publisher.constructor.Do(func() {
		queue, err = channel.QueueDeclare(
			Publisher.GetQueueName(),
			Publisher.queueParams.Durable,
			Publisher.queueParams.AutoDelete,
			Publisher.queueParams.Exclusive,
			Publisher.queueParams.NoWait,
			Publisher.queueParams.Args,
		)
		if err == nil {
			Publisher.queue = &queue
		}
	})
	return err
}

// GetChannel 获取 信道
func (Publisher *SimpleQueuePublisher) GetChannel() *amqp.Channel {
	return Publisher.client.GetChannel()
}

// Close 关闭订阅
func (Publisher *SimpleQueuePublisher) Close() error {
	var err error
	if Publisher.client != nil {
		Publisher.destructor.Do(func() {
			// 停止消费 并关闭
			err = Publisher.client.Close()
			Publisher.client = nil
			Publisher.queue = nil
			Publisher.queueParams = nil
		})
	}
	return err
}
