package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

const (
	// beatChan = make(chan bool, 16)
	beatTime             = time.Second * 30
	heartbeat            = time.Second * 10
	pubTime              = time.Second * 16
	tickTime             = time.Second * 8
	messageTTL           = int64(time.Hour / time.Millisecond)          // TTL for message in queue
	queueExpire          = int64(time.Hour * 24 * 7 / time.Millisecond) // expire time for unused queue
	protocolAmqp         = "amqp"
	protocolAmqpSSL      = "amqps"
	logTag               = "[RABBITMQ_CLIENT]"
	defaultMsgTtl        = "18000" // half hour TTL for message in queue
	defaultContentType   = "application/json"
	defaultContentEncode = "UTF-8"
	defaultPrefetchCount = 1
	defaultPrefetchSize  = 0
	ExchangeTypeDirect   = "direct"
	ExchangeTypeFanOut   = "fanout"
	ExchangeTypeTopic    = "topic"
	ExchangeTypeHeader   = "header"
	defaultQueueName     = "default"
)

type (
	// Client for mq
	Client struct {
		broker      *Broker
		ch          *amqp.Channel
		destructor  sync.Once
		constructor sync.Once
		ctx         context.Context
		cancel      context.CancelFunc
	}
)

// NewClient 构建客户端
func NewClient(broker ...*Broker) *Client {
	var client = &Client{}
	if len(broker) > 0 {
		client.BindBroker(broker[0])
	}
	return client.init()
}

// NewClientWithEnv 通过环境变量构建
func NewClientWithEnv(name string) *Client {
	return NewClient().BindBroker(CreateBrokerByEnv(name))
}

// Close the client
func (clt *Client) Close() error {
	var err error
	clt.destructor.Do(func() {
		clt.cancel()
		if clt.broker == nil {
			return
		}
		err = clt.broker.Close()
		clt.broker = nil
		clt.ch = nil
	})
	return err
}

func (clt *Client) init() *Client {
	clt.constructor = sync.Once{}
	clt.destructor = sync.Once{}
	if clt.ctx == nil || clt.cancel == nil {
		clt.ctx, clt.cancel = context.WithCancel(context.Background())
	}
	return clt
}

// BindBroker 绑定连接器 仅能初始化绑定一次
func (clt *Client) BindBroker(server *Broker) *Client {
	if clt.broker == nil && server != nil {
		clt.constructor.Do(func() {
			clt.broker = server
		})
	}
	return clt
}

// 获取 Broker 链接器
func (clt *Client) GetBroker() *Broker {
	if clt.broker == nil {
		return CreateBrokerByEnv()
	}
	return clt.broker
}

// 设置上下文
func (clt *Client) SetContext(ctx context.Context, cancel context.CancelFunc) *Client {
	clt.ctx, clt.cancel = ctx, cancel
	return clt
}

// 获取信道
func (clt *Client) GetChannel() *amqp.Channel {
	if clt.ch != nil {
		return clt.ch
	}
	var (
		conn = clt.GetBroker().GetConnection()
	)
	ch, err := conn.Channel()
	if err != nil {
		log.Println(logTag, " GetChannel.Error: ", err.Error())
		return nil
	}
	clt.ch = ch
	return ch
}

// Send 发送
func (clt *Client) Send(params MessageParams) error {
	var ch = clt.GetChannel()
	if ch == nil {
		return fmt.Errorf("[RABBITMQ_CLIENT] Error: Channel missing")
	}
	return ch.Publish(params.Exchange, params.Key, params.Mandatory, params.Immediate, params.Msg)
}

// Receive 接收
func (clt *Client) Receive(params ConsumerParams) (<-chan amqp.Delivery, error) {
	var ch = clt.GetChannel()
	if ch == nil {
		return nil, fmt.Errorf("[RABBITMQ_CLIENT] Error: Channel missing")
	}
	var c, err = ch.Consume(
		params.Queue,
		params.Name,
		params.AutoAck,
		params.Exclusive,
		params.NoLocal,
		params.NoWait,
		params.Args,
	)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (clt *Client) SendQueue(queue string, replyTo string, data interface{}) error {
	var (
		msg        []byte
		params     *MessageParams
		publishing *amqp.Publishing
	)
	if data == nil {
		return fmt.Errorf("[RABBITMQ_CLIENT] data nil")
	}
	switch data.(type) {
	case amqp.Delivery:
		msg = data.(amqp.Delivery).Body
	case []byte:
		msg = data.([]byte)
	case string:
		msg = []byte(data.(string))
	case amqp.Publishing:
		m := data.(amqp.Publishing)
		publishing = &m
	case fmt.Stringer:
		msg = []byte(data.(fmt.Stringer).String())
	}
	if msg != nil {
		p := NewSimpleQueueMessageParam(queue, msg)
		params = &p
	}
	if publishing != nil {
		p := NewSimpleQueueMessageParam(queue, nil)
		p.Msg = *publishing
		params = &p
	}
	if params == nil {
		b, err := json.Marshal(data)
		if err != nil {
			return fmt.Errorf("[RABBITMQ_CLIENT] Type Error: %T,%v ,%s", data, data, err.Error())
		}
		p := NewSimpleQueueMessageParam(queue, b)
		params = &p
	}
	params.Msg.ReplyTo = replyTo
	return clt.Send(*params)
}

func (clt *Client) Closed() bool {
	return clt.GetBroker().GetConnection().IsClosed()
}

// Qos 限制 信道 队列 交付数量
func (clt *Client) Qos(params ...*QosParams) error {
	var ch = clt.GetChannel()
	// 默认参数
	params = append(params, &QosParams{
		defaultPrefetchCount,
		defaultPrefetchSize,
		false,
	})
	return ch.Qos(params[0].PrefetchCount, params[0].PrefetchSize, params[0].Global)
}

// 删除队列
func (clt *Client) DeleteQueue(params QueueParams) error {
	var ch = clt.GetChannel()
	_, err := ch.QueueDeclare(
		params.Name,
		params.Durable,
		params.AutoDelete,
		params.Exclusive,
		params.NoWait,
		params.Args,
	)
	if err != nil {
		return err
	}
	return nil
}

// QueueBind 队列绑定 对应 交换器
func (clt *Client) QueueBind(params ExchangeParams) error {
	return clt.GetChannel().QueueBind(params.Name, params.Key, params.Exchange, params.NoWait, params.Args)
}
