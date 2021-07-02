package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"time"
)

type (

	// QueueParams for mq initQueue 队列参数
	QueueParams struct {
		Name         string                 `json:"name"`                 // 队列名
		Durable      bool                   `json:"durable,omitempty"`    // 持久化
		Exchange     string                 `json:"exchange,omitempty"`   // 交换器名
		ExchangeType string                 `json:"type,omitempty"`       // 交换器类型
		Key          string                 `json:"key,omitempty"`        // Router Key
		AutoDelete   bool                   `json:"autoDelete,omitempty"` // 不使用时自动删除
		Exclusive    bool                   `json:"exclusive,omitempty"`  // 排他
		NoWait       bool                   `json:"noWait,omitempty"`     // 不等待
		Args         map[string]interface{} `json:"args,omitempty"`       // 其他参数
	}

	// ConsumerParams for mq createConsumer 参数
	ConsumerParams struct {
		Name         string                 `json:"name"`                // 消费者tag
		Queue        string                 `json:"queue,omitempty"`     // 队列名
		Key          string                 `json:"key,omitempty"`       // Router Key
		Exchange     string                 `json:"exchange,omitempty"`  // 交换器名
		ExchangeType string                 `json:"type,omitempty"`      // 交换器类型
		AutoAck      bool                   `json:"autoAck,omitempty"`   // 自动回复
		Exclusive    bool                   `json:"exclusive,omitempty"` //  排他
		NoLocal      bool                   `json:"noLocal,omitempty"`   //
		NoWait       bool                   `json:"noWait,omitempty"`
		Args         map[string]interface{} `json:"args,omitempty"`
	}

	// ExchangeParams for mq initExchange 参数
	ExchangeParams struct {
		Name       string                 `json:"name"`                     // Queue Name
		Key        string                 `json:"key"`                      // Key
		Type       string                 `json:"type"`                     // Type
		Exchange   string                 `json:"exchange"`                 // 交换器名
		NoWait     bool                   `json:"noWait,omitempty"`         // 是否等等回复
		Durable    bool                   `json:"durable,default=true"`     // 是否持久化
		AutoDelete bool                   `json:"autoDelete,default=false"` // 是否自动删除
		Internal   bool                   `json:"internal,default=false"`   // 是否内部
		Exclusive  bool                   `json:"exclusive,default=false"`  // 排他
		Args       map[string]interface{} `json:"args,omitempty"`           // 参数
	}

	// PubSubParams
	PubSubParams struct {
		Ctx     context.Context // 上下文
		ConnUrl string          // dns 链接 配置
		Cfg     *BrokerCfg      // 配置
		Entry   string          // env 配置 实例对象命名空间
	}

	// 内部业务使用 MessageParams 发送消息参数
	MessageParams struct {
		Key       string          `json:"key,default=''"`                    // 队列
		Exchange  string          `json:"exchange,default=''"`               // 交换机
		Mandatory bool            `json:"mandatory,omitempty,default=false"` // 是否强制
		Immediate bool            `json:"immediate,omitempty,default=false"` // 是否立即
		Msg       amqp.Publishing `json:"msg"`                               // 消息内容体
	}

	// DelParams 删除队列参数列表
	DelParams struct {
		Name     string `json:"name"`     // 队列名|交换器名
		IfUnused bool   `json:"IfUnused"` // 是否未被使用
		IfEmpty  bool   `json:"ifEmpty"`  // 是否空
		NoWait   bool   `json:"noWait"`   // 不等待
	}

	// 外部业务使用  Message 纯消息体
	Message struct {
		rowData interface{}
	}

	// QosParams 控制消息投放量参数
	QosParams struct {
		PrefetchCount int
		PrefetchSize  int
		Global        bool
	}

	MessageReplier interface {
		Ack(multiple bool) error
		Reject(requeue bool) error
		Nack(multiple, requeue bool) error
	}
)

func (p PubSubParams) GetContext() context.Context {
	if p.Ctx == nil {
		return context.TODO()
	}
	return p.Ctx
}

func (p PubSubParams) GetBrokerCfg() *BrokerCfg {
	if p.ConnUrl != "" {
		if cfg, err := ParseUrlBrokerCfg(p.ConnUrl); err == nil {
			return cfg
		}
	}
	if p.Cfg == nil {
		cfg := GetBrokerInfoByEnv(p.Entry)
		p.Cfg = &cfg
	}
	return p.Cfg
}

// NewSimpleQueueMessageParam 简单队列消息
func NewSimpleQueueMessageParam(queue string, data []byte, options ...func(auth *amqp.Publishing)) MessageParams {
	var params = MessageParams{
		Key:       queue,
		Exchange:  "",
		Mandatory: false,
		Immediate: false,
		Msg: amqp.Publishing{
			Body:            data,
			Expiration:      defaultMsgTtl,
			ContentEncoding: defaultContentEncode,
			ContentType:     defaultContentType,
			Timestamp:       time.Now(),
		},
	}
	if len(options) > 0 {
		for _, opt := range options {
			if opt == nil {
				continue
			}
			opt(&params.Msg)
		}
	}
	return params
}

func NewBytes(v interface{}) []byte {
	if v == nil {
		return nil
	}
	d, err := json.Marshal(v)
	if err == nil {
		return d
	}
	log.Println("[NewBytes] Error:", err.Error(), "v info:", fmt.Sprintf("%T,%v", v, v))
	return nil
}

func NewMessage(data interface{}) *Message {
	return &Message{data}
}

func (m *Message) GetContent() []byte {
	var (
		msg        []byte
		publishing *amqp.Publishing
	)
	if m.rowData == nil {
		return nil
	}
	switch m.rowData.(type) {
	case amqp.Delivery:
		msg = m.rowData.(amqp.Delivery).Body
	case []byte:
		msg = m.rowData.([]byte)
	case string:
		msg = []byte(m.rowData.(string))
	case amqp.Publishing:
		_m := m.rowData.(amqp.Publishing)
		publishing = &_m
	case fmt.Stringer:
		msg = []byte(m.rowData.(fmt.Stringer).String())
	}
	if msg != nil {
		return msg
	}
	if publishing != nil {
		return publishing.Body
	}
	if msg, err := json.Marshal(m.rowData); err == nil {
		return msg
	}
	return nil
}

func (m *Message) String() string {
	return string(m.GetContent())
}

func (m *Message) GetRowMessage() interface{} {
	return m.rowData
}

func NewMessageParams(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) *MessageParams {
	return &MessageParams{
		Exchange:  exchange,
		Key:       key,
		Mandatory: mandatory,
		Immediate: immediate,
		Msg:       msg,
	}
}

func (m *MessageParams) GetContent() []byte {
	if msg, err := json.Marshal(m); err == nil {
		return msg
	}
	return nil
}

func (m *MessageParams) String() string {
	return string(m.GetContent())
}

func (m *MessageParams) GetRowMessage() interface{} {
	return m
}

// ConsumerParams
func (param *ConsumerParams) SetBool(key string, v bool) *ConsumerParams {
	switch key {
	case "AutoAck":
		param.AutoAck = v
	case "autoAck":
		param.AutoAck = v
	case "Exclusive":
		param.Exclusive = v
	case "exclusive":
		param.Exclusive = v
	case "NoLocal":
		param.NoLocal = v
	case "noLocal":
		param.NoLocal = v
	case "NoWait":
		param.NoWait = v
	case "noWait":
		param.NoWait = v
	}
	return param
}

func (param *ConsumerParams) SetString(key string, v string) *ConsumerParams {
	switch key {
	case "Name":
		param.Name = v
	case "name":
		param.Name = v
	case "Queue":
		param.Queue = v
	case "queue":
		param.Queue = v
	case "key":
		param.Key = v
	case "Key":
		param.Key = v
	case "Exchange":
		param.Exchange = v
	case "exchange":
		param.Exchange = v
	case "type":
		if inTypeArray(v) {
			param.ExchangeType = v
		}
	case "ExchangeType":
		if inTypeArray(v) {
			param.ExchangeType = v
		}
	case "Type":
		if inTypeArray(v) {
			param.ExchangeType = v
		}
	}
	return param
}

func (param *ConsumerParams) SetArgs(key string, v interface{}) *ConsumerParams {
	if param.Args == nil {
		param.Args = make(map[string]interface{})
	}
	param.Args[key] = v
	return param
}

func (param *ConsumerParams) RemoveArgs(key string) *ConsumerParams {
	if param.Args == nil {
		param.Args = make(map[string]interface{})
	}
	if _, ok := param.Args[key]; ok {
		delete(param.Args, key)
	}
	return param
}

// QueueParams
func (param *QueueParams) SetBool(key string, v bool) *QueueParams {
	switch key {
	case "AutoDelete":
		param.AutoDelete = v
	case "autoDelete":
		param.AutoDelete = v
	case "Durable":
		param.Durable = v
	case "durable":
		param.Durable = v
	case "Exclusive":
		param.Exclusive = v
	case "exclusive":
		param.Exclusive = v
	case "NoWait":
		param.NoWait = v
	case "noWait":
		param.NoWait = v
	}
	return param
}

func (param *QueueParams) SetString(key string, v string) *QueueParams {
	switch key {
	case "Name":
		param.Name = v
	case "name":
		param.Name = v
	case "key":
		param.Key = v
	case "Key":
		param.Key = v
	case "Exchange":
		param.Exchange = v
	case "exchange":
		param.Exchange = v
	case "type":
		if inTypeArray(v) {
			param.ExchangeType = v
		}
	case "ExchangeType":
		if inTypeArray(v) {
			param.ExchangeType = v
		}
	case "Type":
		if inTypeArray(v) {
			param.ExchangeType = v
		}
	}
	return param
}

func (param *QueueParams) SetArgs(key string, v interface{}) *QueueParams {
	if param.Args == nil {
		param.Args = make(map[string]interface{})
	}
	param.Args[key] = v
	return param
}

func (param *QueueParams) RemoveArgs(key string) *QueueParams {
	if param.Args == nil {
		param.Args = make(map[string]interface{})
	}
	if _, ok := param.Args[key]; ok {
		delete(param.Args, key)
	}
	return param
}

// ExchangeParams
func (param *ExchangeParams) SetBool(key string, v bool) *ExchangeParams {
	switch key {
	case "AutoDelete":
		param.AutoDelete = v
	case "autoDelete":
		param.AutoDelete = v
	case "Durable":
		param.Durable = v
	case "durable":
		param.Durable = v
	case "Exclusive":
		param.Exclusive = v
	case "exclusive":
		param.Exclusive = v
	case "NoWait":
		param.NoWait = v
	case "noWait":
		param.NoWait = v
	case "internal":
		param.Internal = v
	case "Internal":
		param.Internal = v
	}
	return param
}

func (param *ExchangeParams) SetString(key string, v string) *ExchangeParams {
	switch key {
	case "Name":
		param.Name = v
	case "name":
		param.Name = v
	case "key":
		param.Key = v
	case "Key":
		param.Key = v
	case "Exchange":
		param.Exchange = v
	case "exchange":
		param.Exchange = v
	case "type":
		if inTypeArray(v) {
			param.Type = v
		}
	case "ExchangeType":
		if inTypeArray(v) {
			param.Type = v
		}
	case "Type":
		if inTypeArray(v) {
			param.Type = v
		}
	}
	return param
}

func (param *ExchangeParams) SetArgs(key string, v interface{}) *ExchangeParams {
	if param.Args == nil {
		param.Args = make(map[string]interface{})
	}
	param.Args[key] = v
	return param
}

func (param *ExchangeParams) RemoveArgs(key string) *ExchangeParams {
	if param.Args == nil {
		param.Args = make(map[string]interface{})
	}
	if _, ok := param.Args[key]; ok {
		delete(param.Args, key)
	}
	return param
}
