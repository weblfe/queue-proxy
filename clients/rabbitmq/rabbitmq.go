package rabbitmq

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

const (
	// beatChan = make(chan bool, 16)
	beatTime        = time.Second * 30
	heartbeat       = time.Second * 10
	pubTime         = time.Second * 16
	tickTime        = time.Second * 8
	messageTTL      = int64(time.Hour / time.Millisecond)          // TTL for message in queue
	queueExpire     = int64(time.Hour * 24 * 7 / time.Millisecond) // expire time for unused queue
	protocolAmqp    = "amqp"
	protocolAmqpSSL = "amqps"
	logTag          = "[RABBITMQ_CLIENT]"
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

// 构建客户端
func NewClient(broker ...*Broker) *Client {
	if len(broker) > 0 {
		return &Client{
			broker: broker[0],
		}
	}
	return &Client{}
}

// 通过环境变量构建
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
