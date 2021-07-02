package rabbitmq

import (
	"fmt"
	"github.com/streadway/amqp"
	"sync"
	"time"
)

type (
	// 发布/订阅 模式 消费器
	SubscribeConsumer struct {
		SimpleQueueConsumer
		qos                 *QosParams
		params              *ExchangeParams
		constructorExchange sync.Once
	}
)

// 创建交换机参数
func NewExchangeParams(exchange, tye string) *ExchangeParams {
	return &ExchangeParams{
		Name:       "",
		Exchange:   exchange,
		Type:       tye,
		NoWait:     false,
		Durable:    true,
		AutoDelete: false,
		Internal:   false,
		Arguments:  Arguments{make(map[string]interface{})},
	}
}

//  NewRoutingExchangeParams routing 模式 创建交换机参数
func NewRoutingExchangeParams(exchange string) *ExchangeParams {
	return NewExchangeParams(exchange, ExchangeTypeDirect)
}

// 自动 Ack
func NewSubscribeConsumerParams(tag, exchange, ty string) *ConsumerParams {
	return &ConsumerParams{
		Name:         tag,
		Queue:        "",
		Exchange:     exchange,
		ExchangeType: ty,
		AutoAck:      true, // 如需保障业务,设置成false
		Exclusive:    false,
		NoLocal:      false,
		NoWait:       false,
		Arguments:    Arguments{make(map[string]interface{})},
	}
}

// 参数
func ConsumerParamsOf() *ConsumerParams {
	return &ConsumerParams{
		AutoAck:   true, // 如需保障业务,设置成false
		Exclusive: false,
		NoLocal:   false,
		NoWait:    false,
		Arguments: Arguments{make(map[string]interface{})},
	}
}

// 自己 Ack
func NewSubscribeConsumerParamsAck(tag, exchange, ty string) *ConsumerParams {
	return NewSubscribeConsumerParams(tag, exchange, ty).SetBool("AutoAck", false)
}

// NewSubscribeQueueParams 定义模式队列模式 默认 队列参数
func NewSubscribeQueueParams(queue, exchange, ty string) *QueueParams {
	return &QueueParams{
		Name:         queue,
		Exchange:     exchange,
		ExchangeType: ty,
		AutoDelete:   false,
		NoWait:       false,
		Exclusive:    false,
		Durable:      true,
		Arguments:    Arguments{make(map[string]interface{})},
	}
}

// NewSubscribeConsumer 创建定义 消费者[发布/订阅 Publish/Subscribe]
func NewSubscribeConsumer(client *Client, queueParams *QueueParams, consumerParams *ConsumerParams, Qos ...*QosParams) *SubscribeConsumer {
	// 消费队列 和 定义队列必须一致
	if consumerParams.Queue != "" {
		queueParams.Name = consumerParams.Queue
	}
	// 消费者 tag
	if consumerParams.Name == "" {
		consumerParams.Name = fmt.Sprintf(queueParams.Name+".worker.%d", time.Now().Unix())
	}
	Qos = append(Qos, nil)
	consumer := &SubscribeConsumer{
		SimpleQueueConsumer: SimpleQueueConsumer{
			client:         client,
			destructor:     sync.Once{},
			constructor:    sync.Once{},
			queueParams:    queueParams,
			ctrl:           make(chan bool),
			consumerParams: consumerParams,
			ch:             make(chan AckMessageWrapper, 10),
		},
		qos:                 Qos[0],
		constructorExchange: sync.Once{},
	}
	p := consumer.GetExchangeParams()
	consumer.params = &p
	return consumer
}

// Subscribe 订阅processor 启动函数
func (Consumer *SubscribeConsumer) Subscribe() error {
	// 初始化 定义交换机
	if err := Consumer.initExchange(); err != nil {
		return err
	}
	// 初始化 定义队列
	if err := Consumer.initQueue(); err != nil {
		return err
	}
	// 获取消费队列channel
	var output, err = Consumer.getConsumer()
	if err != nil {
		return err
	}
	// 限制投递数量
	if Consumer.qos != nil {
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

// initExchange 初始化 队列
func (Consumer *SubscribeConsumer) initExchange() error {
	var (
		err     error
		channel = Consumer.GetChannel()
	)
	Consumer.constructorExchange.Do(func() {
		var params = Consumer.GetExchangeParams()
		err = channel.ExchangeDeclare(
			params.Exchange,
			params.Type,
			params.Durable,
			params.AutoDelete,
			params.Internal,
			params.NoWait,
			params.Args,
		)
	})
	return err
}

// GetExchangeParams 获取交换机创建参数
func (Consumer *SubscribeConsumer) GetExchangeParams() ExchangeParams {
	if Consumer.params != nil {
		return *Consumer.params
	}
	var (
		exchange = Consumer.consumerParams.Exchange
		_type    = Consumer.consumerParams.ExchangeType
		key      = Consumer.consumerParams.Key
	)
	if Consumer.queueParams.Exchange != "" && exchange == "" {
		exchange = Consumer.queueParams.Exchange
	}
	if Consumer.queueParams.ExchangeType != "" && _type == "" {
		_type = Consumer.queueParams.ExchangeType
	}
	if Consumer.queueParams.Key != "" && key == "" {
		key = Consumer.queueParams.Key
	}
	return ExchangeParams{
		Name:       Consumer.GetQueueName(),
		Key:        key,
		Type:       _type,
		Exchange:   exchange,
		NoWait:     Consumer.consumerParams.NoWait,
		Arguments:  Arguments{Consumer.consumerParams.Args},
		Durable:    Consumer.queueParams.Durable,
		AutoDelete: Consumer.queueParams.AutoDelete,
	}
}

// getConsumer 获取消息channel
func (Consumer *SubscribeConsumer) getConsumer() (<-chan amqp.Delivery, error) {
	var params = Consumer.GetExchangeParams()
	// params.
	if err := Consumer.client.QueueBind(params); err != nil {
		return nil, err
	}
	Consumer.consumerParams.Queue = Consumer.GetQueueName()
	return Consumer.client.Receive(*Consumer.consumerParams)
}

// DeleteExchange 删除交换器
func (Consumer *SubscribeConsumer) DeleteExchange(opts ...func(params *DelParams)) error {
	var params = DelParams{
		Name: Consumer.params.Exchange,
	}
	if len(opts) > 0 {
		for _, setter := range opts {
			if setter == nil {
				continue
			}
			setter(&params)
		}
	}
	return Consumer.client.DeleteExchange(params)
}
