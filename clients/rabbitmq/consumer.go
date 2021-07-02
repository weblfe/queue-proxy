package rabbitmq

import (
	"encoding/json"
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
		constructor    sync.Once
		ch             chan AckMessageWrapper
		ctrl           chan bool
		queueParams    *QueueParams
		consumerParams *ConsumerParams
		queue          *amqp.Queue
		output         <-chan amqp.Delivery
		qosOn          bool
	}

	Consumer interface {
		Consume() <-chan AckMessageWrapper
		Close() error
		Subscribe() error
	}

	MessageWrapper interface {
		GetContent() []byte
		GetRowMessage() interface{}
		fmt.Stringer
	}

	AckMessageWrapper interface {
		MessageWrapper
		MessageReplier
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
		NoWait:     false,
		Exclusive:  false,
		Durable:    true,
		Arguments:  Arguments{make(map[string]interface{})},
	}
}

// NewSimpleConsumerParams 简单队列模式 默认 消费者参数
func NewSimpleConsumerParams(name string, queue string) *ConsumerParams {
	return &ConsumerParams{
		Name:      name,
		Queue:     queue,
		AutoAck:   true,
		NoWait:    false,
		Exclusive: false,
		NoLocal:   false,
		Arguments: Arguments{make(map[string]interface{})},
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

// String 转换成字符串
func (wrapper *AmqpMessageWrapper) String() string {
	return string(wrapper.GetContent())
}

// 解码
func (wrapper *AmqpMessageWrapper) Decode(v interface{}) error {
	if v == nil {
		return fmt.Errorf("Decode.Error: Nil Pointer")
	}
	data := wrapper.GetContent()
	if len(data) > 0 {
		return json.Unmarshal(data, v)
	}
	return nil
}

// GetRowMessage 获取原始消息 对象
func (wrapper *AmqpMessageWrapper) GetRowMessage() interface{} {
	return wrapper.rowData
}

func (wrapper *AmqpMessageWrapper) Ack(multiple bool) error {
	return Ack(wrapper, multiple)
}

func (wrapper *AmqpMessageWrapper) Reject(requeue bool) error {
	return Reject(wrapper, requeue)
}

func (wrapper *AmqpMessageWrapper) Nack(multiple, requeue bool) error {
	return Nack(wrapper, multiple, requeue)
}

func NewSimpleQueueConsumer(client *Client, queueParams *QueueParams, consumerParams *ConsumerParams) *SimpleQueueConsumer {
	// 消费队列 和 定义队列必须一致
	if consumerParams.Queue != "" {
		queueParams.Name = consumerParams.Queue
	}
	// 消费者 tag
	if consumerParams.Name == "" {
		consumerParams.Name = fmt.Sprintf(queueParams.Name+".worker.%d", time.Now().Unix())
	}
	return &SimpleQueueConsumer{
		client:         client,
		destructor:     sync.Once{},
		constructor:    sync.Once{},
		queueParams:    queueParams,
		ctrl:           make(chan bool),
		consumerParams: consumerParams,
		ch:             make(chan AckMessageWrapper, 10),
	}
}

// Consume 获取消息消费队列
func (Consumer *SimpleQueueConsumer) Consume() <-chan AckMessageWrapper {
	return Consumer.ch
}

// initQueue 初始化 队列
func (Consumer *SimpleQueueConsumer) initQueue() error {
	var (
		err   error
		queue amqp.Queue
	)
	if Consumer.queue == nil {
		Consumer.constructor.Do(func() {
			queue, err = Consumer.Declare()
			if err == nil {
				Consumer.queue = &queue
			}
		})
	}
	return err
}

func (Consumer *SimpleQueueConsumer) SetQos(o bool) {
	Consumer.qosOn = o
}

// Declare 声明队列
func (Consumer *SimpleQueueConsumer) Declare() (amqp.Queue, error) {
	var (
		channel = Consumer.GetChannel()
	)
	return channel.QueueDeclare(
		Consumer.GetQueueName(),
		Consumer.queueParams.Durable,
		Consumer.queueParams.AutoDelete,
		Consumer.queueParams.Exclusive,
		Consumer.queueParams.NoWait,
		Consumer.queueParams.Args,
	)
}

// GetChannel 获取 信道
func (Consumer *SimpleQueueConsumer) GetChannel() *amqp.Channel {
	return Consumer.client.GetChannel()
}

// Subscribe 订阅processor 启动函数
func (Consumer *SimpleQueueConsumer) Subscribe() error {
	// 检查队列是否初始化
	if err := Consumer.initQueue(); err != nil {
		return err
	}
	// 获取消费队列channel
	var output, err = Consumer.getConsumer()
	if err != nil {
		return err
	}
	// 是开启 限制投递数量
	if Consumer.qosOn {
		if err = Consumer.client.Qos(); err != nil {
			return err
		}
	}
	for {
		select {
		case msg := <-output:
			Consumer.push(msg)
		case v := <-Consumer.ctrl:
			if v {
				return Consumer.recycle()
			}
		}
	}
}

// push 推到消费进程 channel
func (Consumer *SimpleQueueConsumer) push(delivery amqp.Delivery) {
	if len(Consumer.ch) >= cap(Consumer.ch) {
		log.Println("[Consumer.Push ] Channel full")
	}
	Consumer.ch <- NewMessageWrapper(delivery)
}

// recycle 回收
func (Consumer *SimpleQueueConsumer) recycle() error {
	if len(Consumer.ch) > 0 {
		var queue = Consumer.GetQueueName()
		for v := range Consumer.ch {
			if v == nil {
				continue
			}
			data := v.GetRowMessage()
			if data == nil {
				continue
			}
			if err := Consumer.client.SendToQueue(queue, Consumer.queue.Name, data); err != nil {
				log.Println("[Consumer.Save] Error:", err.Error(), "msg: ", data)
			}
		}
	}
	return nil
}

// 获取队列名
func (Consumer *SimpleQueueConsumer) GetQueueName() string {
	if Consumer.queueParams.Name != "" {
		return Consumer.queueParams.Name
	}
	return Consumer.consumerParams.Queue
}

// 获取消息channel
func (Consumer *SimpleQueueConsumer) getConsumer() (<-chan amqp.Delivery, error) {
	Consumer.consumerParams.Queue = Consumer.GetQueueName()
	return Consumer.client.Receive(*Consumer.consumerParams)
}

// Close 关闭订阅
func (Consumer *SimpleQueueConsumer) Close() error {
	var err error
	if Consumer.client != nil {
		Consumer.destructor.Do(func() {
			// 通知关闭
			Consumer.ctrl <- true
			// 停止消费 并关闭
			err = Consumer.client.Close()
			Consumer.queue = nil
			Consumer.client = nil
			Consumer.queueParams = nil
			Consumer.consumerParams = nil
		})
	}
	return err
}

// Delete 删除队列
func (Consumer *SimpleQueueConsumer) Delete(opts ...func(params *DelParams)) error {
	var params = DelParams{
		Name: Consumer.GetQueueName(),
	}
	if len(opts) > 0 {
		for _, setter := range opts {
			if setter == nil {
				continue
			}
			setter(&params)
		}
	}
	return Consumer.client.DeleteQueue(params)
}
