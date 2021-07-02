package rabbitmq

import (
	"fmt"
	"github.com/streadway/amqp"
)

type (
	// 路由Routing 模式 消费器
	RoutingConsumer struct {
		SubscribeConsumer
	}
)

// NewRoutingConsumerParamsAck 自己 Ack
func NewRoutingConsumerParamsAck(tag, exchange, key string) *ConsumerParams {
	return NewSubscribeConsumerParams(tag, exchange, ExchangeTypeDirect).
		SetString("Key", key).
		SetBool("AutoAck", false)
}

// NewRoutingConsumerParams auto Ack
func NewRoutingConsumerParams(tag, exchange, key string) *ConsumerParams {
	return NewSubscribeConsumerParams(tag, exchange, ExchangeTypeDirect).
		SetString("Key", key)
}

// NewRoutingQueueParams 定义模式队列模式 默认 队列参数
func NewRoutingQueueParams(queue, exchange, key string) *QueueParams {
	return &QueueParams{
		Name:         queue,
		Exchange:     exchange,
		ExchangeType: ExchangeTypeDirect,
		Key:          key,
		AutoDelete:   false,
		NoWait:       false,
		Exclusive:    false,
		Durable:      true,
		Arguments:    Arguments{make(map[string]interface{})},
	}
}

// NewSubscribeConsumer 创建定义 消费者[Routing Publish/Subscribe]
func NewRoutingConsumer(client *Client, queueParams *QueueParams, consumerParams *ConsumerParams, Qos ...*QosParams) *RoutingConsumer {
	if consumerParams.Key == "" {
		consumerParams.Key = defaultRoutingKey
	}
	consumer := &RoutingConsumer{
		SubscribeConsumer: *NewSubscribeConsumer(client, queueParams, consumerParams, Qos...),
	}
	return consumer
}

// Subscribe 订阅processor 启动函数
func (Consumer *RoutingConsumer) Subscribe() error {
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
func (Consumer *RoutingConsumer) initExchange() error {
	var (
		err     error
		channel = Consumer.GetChannel()
	)
	Consumer.constructorExchange.Do(func() {
		var params = Consumer.GetExchangeParams()
		err = channel.ExchangeDeclare(
			params.Exchange,
			Consumer.GetExchangeType(),
			params.Durable,
			params.AutoDelete,
			params.Internal,
			params.NoWait,
			params.Args,
		)
	})
	return err
}

// GetExchangeType 当前交换器类型
func (Consumer *RoutingConsumer) GetExchangeType() string {
	if Consumer.params == nil {
		return ExchangeTypeDirect
	}
	if Consumer.params.Type != ExchangeTypeDirect {
		Consumer.params.Type = ExchangeTypeDirect
	}
	return Consumer.params.Type
}

// 获取交换器创建配置
func (Consumer *RoutingConsumer) GetExchangeParams() ExchangeParams {
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
	if _type == "" {
		_type = ExchangeTypeDirect
	}
	if Consumer.queueParams.Key != "" && key == "" {
		key = Consumer.queueParams.Key
	}
	if key == "" {
		key = defaultRoutingKey
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
func (Consumer *RoutingConsumer) getConsumer() (<-chan amqp.Delivery, error) {
	var params = Consumer.GetExchangeParams()
	// 必须 有key
	if params.Key == "" {
		params.Key = defaultRoutingKey
	}
	// 检查 交换器
	if params.Exchange == "" {
		return nil, fmt.Errorf("[RoutingConsumer.getConsumer] Error: %s", "Required Exchange Name But Empty Given")
	}
	// 绑定 队列 到交换器
	if err := Consumer.client.QueueBind(params); err != nil {
		return nil, err
	}
	Consumer.consumerParams.Queue = Consumer.GetQueueName()
	return Consumer.client.Receive(*Consumer.consumerParams)
}
