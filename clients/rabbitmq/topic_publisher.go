package rabbitmq

import (
	"fmt"
)

type (
	// Topic 模式 发布器
	TopicPublisher struct {
		RoutingPublisher
	}
)

// 获取 topic Exchange 参数
func NewTopicExchangeParams(exchange string) *ExchangeParams{
	return NewExchangeParams(exchange, ExchangeTypeTopic)
}

// NewTopicPublisher 构建Topic　推送器
func NewTopicPublisher(client *Client, params *ExchangeParams) *TopicPublisher {
	if params == nil {
		return nil
	}
	// Direct
	if params.Type == "" {
		params.Type = ExchangeTypeTopic
	}
	return &TopicPublisher{
		RoutingPublisher: *NewRoutingPublisher(client, params),
	}
}

// 获取路由
func (Publisher *TopicPublisher) GetRoutingKey() string {
	if Publisher.params.Key == "" {
		return defaultRoutingKey
	}
	return Publisher.params.Key
}

// Send 发送消息
func (Publisher *TopicPublisher) Send(message MessageWrapper) error {
	if Publisher.client.Closed() {
		return fmt.Errorf("[Publisher] Client Connection Closed")
	}
	// 交换器 初始化
	if err := Publisher.initExchange(); err != nil {
		return err
	}
	if message == nil {
		return fmt.Errorf("[PublishPublisher] Client message is Nil")
	}
	// 是否 msg params
	if msg := messageParamsFor(message); msg != nil {
		if msg.Key == "" {
			msg.Key = Publisher.GetRoutingKey()
		}
		if msg.Exchange == "" {
			msg.Exchange = Publisher.GetExchangeName()
		}
		return Publisher.client.Send(*msg)
	}
	// 构造 消息参数体
	var (
		row      = message.GetRowMessage()
		exchange = Publisher.GetExchangeName()
		msg      = MessageParams{
			Exchange:  exchange,
			Mandatory: false,
			Immediate: false,
			Key:       Publisher.GetRoutingKey(),
			Msg:       CreatePublishing(row),
		}
	)
	return Publisher.client.Send(msg)
}

// Emit 发送消息
func (Publisher *TopicPublisher) Emit(message MessageWrapper, routingKey ...string) error {
	if Publisher.client.Closed() {
		return fmt.Errorf("[Publisher] Client Connection Closed")
	}
	// 交换器 初始化
	if err := Publisher.initExchange(); err != nil {
		return err
	}
	if message == nil {
		return fmt.Errorf("[PublishPublisher] Client message is Nil")
	}
	routingKey = append(routingKey, Publisher.GetRoutingKey())
	// 是否 msg params
	if msg := messageParamsFor(message); msg != nil {
		msg.Key = routingKey[0]
		return Publisher.client.Send(*msg)
	}
	// 构造 消息参数体
	var (
		row      = message.GetRowMessage()
		exchange = Publisher.GetExchangeName()
		msg      = MessageParams{
			Mandatory: false,
			Immediate: false,
			Exchange:  exchange,
			Key:       routingKey[0],
			Msg:       CreatePublishing(row),
		}
	)
	return Publisher.client.Send(msg)
}

// 获取路由 类型
func (Publisher *TopicPublisher) GetExchangeType() string {
	if Publisher.params.Type != ExchangeTypeTopic {
		Publisher.params.Type = ExchangeTypeTopic
	}
	return Publisher.params.Type
}

// initExchange 初始化 队列
func (Publisher *TopicPublisher) initExchange() error {
	var (
		err     error
		channel = Publisher.GetChannel()
	)
	Publisher.constructorExchange.Do(func() {
		var params = Publisher.params
		err = channel.ExchangeDeclare(
			params.Exchange,
			Publisher.GetExchangeType(),
			params.Durable,
			params.AutoDelete,
			params.Internal,
			params.NoWait,
			params.Args,
		)
	})
	return err
}
