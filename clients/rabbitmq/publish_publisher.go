package rabbitmq

import (
	"fmt"
	"sync"
)

type (
	// 发布/订阅 模式 发布器
	PublishPublisher struct {
		SimpleQueuePublisher
		params              *ExchangeParams
		constructorExchange sync.Once
	}
)

func NewPublishPublisher(client *Client, params *ExchangeParams) *PublishPublisher {
	var queueParams = QueueParams{
		Name:         params.Name,
		Durable:      params.Durable,
		Exchange:     params.Exchange,
		ExchangeType: params.Type,
		Key:          params.Key,
		AutoDelete:   params.AutoDelete,
		Exclusive:    params.Exclusive,
		NoWait:       params.NoWait,
		Arguments:    Arguments{params.Args},
	}
	return &PublishPublisher{
		SimpleQueuePublisher: SimpleQueuePublisher{
			client:      client,
			destructor:  sync.Once{},
			constructor: sync.Once{},
			queueParams: &queueParams,
		},
		params:              params,
		constructorExchange: sync.Once{},
	}
}

// 发送消息
func (Publisher *PublishPublisher) Send(message MessageWrapper) error {
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
		msg.Key = ""
		msg.Exchange = Publisher.GetExchangeName()
		return Publisher.client.Send(*msg)
	}
	// 构造 消息参数体
	var (
		row      = message.GetRowMessage()
		exchange = Publisher.GetExchangeName()
		msg      = MessageParams{
			Key:       "",
			Exchange:  exchange,
			Mandatory: false,
			Immediate: false,
			Msg:       CreatePublishing(row),
		}
	)
	// msg.Msg.ReplyTo = Publisher.GetQueueName()
	return Publisher.client.Send(msg)
}

// 获取 交换器名
func (Publisher *PublishPublisher) GetExchangeName() string {
	return Publisher.params.Exchange
}

// initExchange 初始化 队列
func (Publisher *PublishPublisher) initExchange() error {
	var (
		err     error
		channel = Publisher.GetChannel()
	)
	Publisher.constructorExchange.Do(func() {
		var params = Publisher.params
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

// Close 关闭订阅
func (Publisher *PublishPublisher) Close() error {
	var err error
	err = Publisher.SimpleQueuePublisher.Close()
	Publisher.params = nil
	return err
}

// DeleteExchange 删除 交换机
func (Publisher *PublishPublisher) DeleteExchange(opts ...func(params *DelParams)) error {
	var params = DelParams{}
	if len(opts) > 0 {
		for _, setter := range opts {
			if setter == nil {
				continue
			}
			setter(&params)
		}
	}
	params.Name = Publisher.GetExchangeName()
	return Publisher.client.DeleteExchange(params)
}
