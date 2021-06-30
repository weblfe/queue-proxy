package rabbitmq

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"sync"
	"time"
)

type (
	SimpleQueueConsumer struct {
		client         *Client
		destructor     sync.Once
		ch             chan MessageWrapper
		ctrl           chan bool
		queueParams    *QueueParams
		consumerParams *ConsumerParams
		queue          *amqp.Queue
		output         <-chan amqp.Delivery
	}

	Consumer interface {
		Consume() chan<- MessageWrapper
		Close() error
		Subscribe() error
	}

	MessageWrapper interface {
		GetContent() []byte
		GetRowMessage() interface{}
	}

	AmqpMessageWrapper struct {
		rowData *amqp.Delivery
	}
)

// NewSimpleQueueParams 简单队列模式 默认 队列参数
func NewSimpleQueueParams(name string) *QueueParams {
	return &QueueParams{
		Name:       name,
		AutoDelete: false,
		NoWait:     true,
		Exclusive:  false,
		Durable:    false,
		Args:       make(map[string]interface{}),
	}
}

// NewSimpleConsumerParams 简单队列模式 默认 消费者参数
func NewSimpleConsumerParams(name string) *ConsumerParams {
	return &ConsumerParams{
		Name:      name,
		AutoAck:   true,
		NoWait:    true,
		Exclusive: false,
		NoLocal:   false,
		Args:      make(map[string]interface{}),
	}
}

// NewMessageWrapper 消息封装器
func NewMessageWrapper(data amqp.Delivery) *AmqpMessageWrapper {
	return &AmqpMessageWrapper{
		rowData: &data,
	}
}

// GetContent 获取消息体
func (wrapper *AmqpMessageWrapper) GetContent() []byte {
	if wrapper.rowData == nil {
		return nil
	}
	return wrapper.rowData.Body
}

// GetRowMessage 获取原始消息 对象
func (wrapper *AmqpMessageWrapper) GetRowMessage() interface{} {
	return wrapper.rowData
}

func NewSimpleQueueConsumer(client *Client, queueParams *QueueParams, consumerParams *ConsumerParams) *SimpleQueueConsumer {
	if consumerParams.Name == "" {
		consumerParams.Name = fmt.Sprintf(queueParams.Name+".worker.%d", time.Now().Unix())
	}
	return &SimpleQueueConsumer{
		client:         client,
		destructor:     sync.Once{},
		ch:             make(chan MessageWrapper, 10),
		ctrl:           make(chan bool),
		queueParams:    queueParams,
		consumerParams: consumerParams,
	}
}

// Consume 获取消息消费队列
func (Consumer *SimpleQueueConsumer) Consume() chan<- MessageWrapper {
	return Consumer.ch
}

// initQueue 初始化 队列
func (Consumer *SimpleQueueConsumer) initQueue() error {
	var (
		err     error
		queue   amqp.Queue
		channel = Consumer.GetChannel()
	)
	queue, err = channel.QueueDeclare(
		Consumer.queueParams.Name,
		Consumer.queueParams.Durable,
		Consumer.queueParams.AutoDelete,
		Consumer.queueParams.Exclusive,
		Consumer.queueParams.NoWait, Consumer.queueParams.Args,
	)
	if err == nil {
		Consumer.queue = &queue
	}
	return err
}

// GetChannel 获取 信道
func (Consumer *SimpleQueueConsumer) GetChannel() *amqp.Channel {
	return Consumer.client.GetChannel()
}

// Subscribe 订阅processor 启动函数
func (Consumer *SimpleQueueConsumer) Subscribe() error {
	if Consumer.queue == nil {
		if err := Consumer.initQueue(); err != nil {
			return err
		}
	}
	// 获取消费队列channel
	var output, err = Consumer.getConsumer()
	if err != nil {
		return err
	}
	for {
		select {
		case msg := <-output:
			Consumer.push(msg)
		case v := <-Consumer.ctrl:
			if v {
				return nil
			}
		}
	}
}

func (Consumer *SimpleQueueConsumer) push(delivery amqp.Delivery) {
	if len(Consumer.ch) >= cap(Consumer.ch) {
		log.Println("[Consumer.Push ] Channel full")
		// @todo
	}
	Consumer.ch <- NewMessageWrapper(delivery)
}

func (Consumer *SimpleQueueConsumer) getConsumer() (<-chan amqp.Delivery, error) {
	if Consumer.output != nil {
		return Consumer.output, nil
	}
	var ch = Consumer.GetChannel()
	if ch == nil {
		return nil, fmt.Errorf("[Consumer.Simple] %s", "Get Channel Failed")
	}
	var c, err = ch.Consume(
		Consumer.queueParams.Name,
		Consumer.consumerParams.Name,
		Consumer.consumerParams.AutoAck,
		Consumer.consumerParams.Exclusive,
		Consumer.consumerParams.NoLocal,
		Consumer.consumerParams.NoWait,
		Consumer.consumerParams.Args,
	)
	if err != nil {
		return nil, err
	}
	Consumer.output = c
	return c, nil
}

// Close 关闭订阅
func (Consumer *SimpleQueueConsumer) Close() error {
	var err error
	if Consumer.client != nil {
		Consumer.destructor.Do(func() {
			err = Consumer.client.Close()
			Consumer.client = nil
			Consumer.ctrl <- true
			Consumer.queue = nil
		})
	}
	return err
}
