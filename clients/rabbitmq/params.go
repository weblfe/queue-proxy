package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"math"
	"time"
)

type (

	// QueueParams for mq initQueue 队列参数
	QueueParams struct {
		Name         string `json:"name"`                 // 队列名
		Durable      bool   `json:"durable,omitempty"`    // 持久化
		Exchange     string `json:"exchange,omitempty"`   // 交换器名
		ExchangeType string `json:"type,omitempty"`       // 交换器类型
		Key          string `json:"key,omitempty"`        // Router Key
		AutoDelete   bool   `json:"autoDelete,omitempty"` // 不使用时自动删除
		Exclusive    bool   `json:"exclusive,omitempty"`  // 排他
		NoWait       bool   `json:"noWait,omitempty"`     // 是否非阻塞
		Arguments
	}

	// ConsumerParams for mq createConsumer 参数
	ConsumerParams struct {
		Name         string `json:"name"`                // 消费者tag
		Queue        string `json:"queue,omitempty"`     // 队列名
		Key          string `json:"key,omitempty"`       // Router Key
		Exchange     string `json:"exchange,omitempty"`  // 交换器名
		ExchangeType string `json:"type,omitempty"`      // 交换器类型
		AutoAck      bool   `json:"autoAck,omitempty"`   // 自动回复
		Exclusive    bool   `json:"exclusive,omitempty"` //  排他
		NoLocal      bool   `json:"noLocal,omitempty"`   //
		NoWait       bool   `json:"noWait,omitempty"`    // 是否非阻塞
		Arguments
	}

	// ExchangeParams for mq initExchange 参数
	ExchangeParams struct {
		Name       string `json:"name"`                     // Queue Name
		Key        string `json:"key"`                      // Key
		Type       string `json:"type"`                     // Type
		Exchange   string `json:"exchange"`                 // 交换器名
		NoWait     bool   `json:"noWait,omitempty"`         // 是否非阻塞
		Durable    bool   `json:"durable,default=true"`     // 是否持久化
		AutoDelete bool   `json:"autoDelete,default=false"` // 是否自动删除
		Internal   bool   `json:"internal,default=false"`   // 是否内部
		Exclusive  bool   `json:"exclusive,default=false"`  // 排他
		Arguments
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
		Immediate bool            `json:"immediate,omitempty,default=false"` // 消息
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

	//  prefetchCount: 会告诉RabbitMQ不要同时给一个消费者推送多于N个消息，即一旦有N个消息还没有ack，则该consumer将block掉，直到有消息ack
	//	prefetchSize: 最多传输的内容的大小的限制，0为不限制，但据说prefetchSize参数，rabbitmq没有实现
	//  global: true|false 是否将上面设置应用于channel，简单点说，就是上面限制是channel级别的还是consumer级别
	// QosParams 控制消息投放量参数
	QosParams struct {
		PrefetchCount int  `json:"prefetchCount"`
		PrefetchSize  int  `json:"prefetchSize"`
		Global        bool `json:"global"`
	}

	MessageReplier interface {
		Ack(multiple bool) error
		Reject(requeue bool) error
		Nack(multiple, requeue bool) error
	}

	Arguments struct {
		Args map[string]interface{} `json:"args,omitempty"`
	}
)

const (
	ArgMsgTtlKey            = "x-message-ttl"
	ArgQueueExpires         = "x-expires"
	ArgQueueMaxLen          = "x-max-length"
	ArgQueueMaxLenBytes     = "x-max-length-bytes"
	ArgDeadLetterExchange   = "x-dead-letter-exchange"
	ArgDeadLetterRoutingKey = "x-dead-letter-routing-key"
	ArgMaxPriority          = "x-max-priority"
	ArgQueueMode            = "x-queue-mode"
	ArgQueueMasterLocator   = "x-queue-master-locator"
	ModeLazy                = "lazy"
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

// MessageParamsOf 指定 交换器 和 routing Key 的消息
func MessageParamsOf(exchange, key string, msg interface{}) *MessageParams {
	return &MessageParams{
		Exchange:  exchange,
		Key:       key,
		Mandatory: false,
		Immediate: false,
		Msg:       CreatePublishing(msg),
	}
}

//  MessageParamsCreate 创建仅有消息体的消息参数
func MessageParamsCreate(msg interface{}) *MessageParams {
	return MessageParamsOf("", "", msg)
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

func (m *MessageParams) SetMsgDelivery() *MessageParams {
	m.Msg.DeliveryMode = amqp.Persistent
	return m
}

func (m *MessageParams)SetKey(key string) *MessageParams {
	m.Key = key
	return m
}

func (m *MessageParams)SetExchange(exchange string) *MessageParams {
	m.Exchange = exchange
	return m
}

func (m *MessageParams) SetMsgTransient() *MessageParams {
	m.Msg.DeliveryMode = amqp.Transient
	return m
}

func (m *MessageParams) SetReplyTo(replyTo string) *MessageParams {
	m.Msg.ReplyTo = replyTo
	return m
}

func (m *MessageParams) SetEncoding(encoding string) *MessageParams {
	m.Msg.ContentEncoding = encoding
	return m
}

func (m *MessageParams) SetContentType(ty string) *MessageParams {
	m.Msg.ContentType = ty
	return m
}

// SetPriority 优先级
func (m *MessageParams) SetPriority(priority uint8) *MessageParams {
	m.Msg.Priority = priority
	return m
}

// SetType            string    // message type name
func (m *MessageParams) SetType(ty string) *MessageParams {
	m.Msg.Type = ty
	return m
}

//	UserId          string    // creating user id - ex: "guest"
func (m *MessageParams) SetUserId(uid string) *MessageParams {
	m.Msg.UserId = uid
	return m
}

//	AppId           string    // creating application id
func (m *MessageParams) SetAppId(appid string) *MessageParams {
	m.Msg.AppId = appid
	return m
}

// CorrelationId
func (m *MessageParams) SetCorrelationId(id string) *MessageParams {
	m.Msg.CorrelationId = id
	return m
}

// SetMessageId 消息ID
func (m *MessageParams) SetMessageId(id string) *MessageParams {
	m.Msg.MessageId = id
	return m
}

// SetTimestamp 时间
func (m *MessageParams) SetTimestamp(t time.Time) *MessageParams {
	m.Msg.Timestamp = t
	return m
}

func (m *MessageParams) SetExpiration(exp string) *MessageParams {
	m.Msg.Expiration = exp
	return m
}

// SetBody 设置消息体
func (m *MessageParams) SetBody(body interface{}) *MessageParams {
	if body == nil {
		return m
	}
	var (
		err error
		b   []byte
	)
	switch body.(type) {
	case string:
		b = []byte(body.(string))
	case fmt.Stringer:
		b = []byte(body.(fmt.Stringer).String())
	default:
		b, err = json.Marshal(body)
	}
	if err != nil {
		return m
	}
	m.Msg.Body = b
	return m
}

func (m *MessageParams) AppendHeader(key string, v interface{}) *MessageParams {
	m.Msg.Headers[key] = v
	return m
}

func (m *MessageParams) RemoveHeader(key string) *MessageParams {
	if _, ok := m.Msg.Headers[key]; ok {
		delete(m.Msg.Headers, key)
	}
	return m
}

func (m *MessageParams) GetHeader(key string) interface{} {
	if v, ok := m.Msg.Headers[key]; ok {
		return v
	}
	return nil
}

// SetBool 设置相关bool 参数
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

// SetString 设置相关String 参数
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
		if CheckTypeArray(v) {
			param.ExchangeType = v
		}
	case "ExchangeType":
		if CheckTypeArray(v) {
			param.ExchangeType = v
		}
	case "Type":
		if CheckTypeArray(v) {
			param.ExchangeType = v
		}
	}
	return param
}

// SetArgs 设置扩展参数 Args中的值
func (param *ConsumerParams) SetArgs(key string, v interface{}) *ConsumerParams {
	param.Arguments.SetArgs(key, v)
	return param
}

func (param *ConsumerParams) RemoveArgs(key string) *ConsumerParams {
	param.Arguments.RemoveArgs(key)
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
		if CheckTypeArray(v) {
			param.ExchangeType = v
		}
	case "ExchangeType":
		if CheckTypeArray(v) {
			param.ExchangeType = v
		}
	case "Type":
		if CheckTypeArray(v) {
			param.ExchangeType = v
		}
	}
	return param
}

func (param *QueueParams) SetArgs(key string, v interface{}) *QueueParams {
	param.Arguments.SetArgs(key, v)
	return param
}

func (param *QueueParams) RemoveArgs(key string) *QueueParams {
	param.Arguments.RemoveArgs(key)
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
		if CheckTypeArray(v) {
			param.Type = v
		}
	case "ExchangeType":
		if CheckTypeArray(v) {
			param.Type = v
		}
	case "Type":
		if CheckTypeArray(v) {
			param.Type = v
		}
	}
	return param
}

func (param *ExchangeParams) SetArgs(key string, v interface{}) *ExchangeParams {
	param.Arguments.SetArgs(key, v)
	return param
}

func (param *ExchangeParams) RemoveArgs(key string) *ExchangeParams {
	param.Arguments.RemoveArgs(key)
	return param
}

// Arguments
func (arguments *Arguments) SetArgs(key string, v interface{}) *Arguments {
	if arguments.Args == nil {
		arguments.Args = make(map[string]interface{})
	}
	arguments.Args[key] = v
	return arguments
}

func (arguments *Arguments) RemoveArgs(key string) *Arguments {
	if arguments.Args == nil {
		arguments.Args = make(map[string]interface{})
	}
	if _, ok := arguments.Args[key]; ok {
		delete(arguments.Args, key)
	}
	return arguments
}

//  设置消息过期时间
// 设置队列中的所有消息的生存周期(统一为整个队列的所有消息设置生命周期),
// SetMsgTTL 也可以在发布消息的时候单独为某个消息指定剩余生存时间,单位毫秒, 类似于redis中的ttl，生存时间到了，消息会被从队里中删除
func (arguments *Arguments) SetMsgTTL(ttl time.Duration) {
	if ttl <= 0 {
		return
	}
	arguments.SetArgs(ArgMsgTtlKey, int64(ttl/time.Millisecond))
}

// SetQueueExpires  当队列在指定的时间没有被访问(consume, basicGet, queueDeclare…)就会被删除
func (arguments *Arguments) SetQueueExpires(ttl time.Duration) {
	if ttl <= 0 || ttl > maxMsgTTL {
		return
	}
	arguments.SetArgs(ArgQueueExpires, int64(ttl/time.Millisecond))
}

// SetQueueMaxLength  限定队列的消息的最大值长度，超过指定长度将会把最早的几条删除掉， 类似于mongodb中的固定集合，例如保存最新的100条消息
func (arguments *Arguments) SetQueueMaxLength(length int) {
	if length <= 0 || length >= math.MaxInt32 {
		return
	}
	arguments.SetArgs(ArgQueueMaxLen, length)
}

// SetQueueMaxLengthBytes  (字节数 bit)限定队列最大占用的空间大小， 一般受限于内存、磁盘的大小
func (arguments *Arguments) SetQueueMaxLengthBytes(size int64) {
	if size <= 0 || size >= math.MaxInt32 {
		return
	}
	arguments.SetArgs(ArgQueueMaxLenBytes, size)
}

// SetDeadLetterExchange  (死信指定) 当队列消息长度大于最大长度、或者过期的等，将从队列中删除的消息推送到指定的交换机中去而不是丢弃掉
func (arguments *Arguments) SetDeadLetterExchange(exchange string) {
	if exchange == "" {
		return
	}
	arguments.SetArgs(ArgDeadLetterExchange, exchange)
}

// SetDeadLetterRoutingKey  (删除监听) 将删除的消息推送到指定交换机的指定路由键的队列中去
func (arguments *Arguments) SetDeadLetterRoutingKey(router string) {
	if router == "" {
		return
	}
	arguments.SetArgs(ArgDeadLetterRoutingKey, router)
}

// SetMaxPriority  (队列优先级)
// 优先级队列，声明队列时先定义最大优先级值(定义最大值一般不要太大)，
// 在发布消息的时候指定该消息的优先级， 优先级更高（数值更大的）的消息先被消费,
func (arguments *Arguments) SetMaxPriority(priority int) {
	if priority < 0 || priority >= math.MaxInt32 {
		return
	}
	arguments.SetArgs(ArgMaxPriority, priority)
}

// SetQueueMode 设置 队列模式
// lazy: 先将消息保存到磁盘上，不放在内存中，当消费者开始消费的时候才加载到内存中
func (arguments *Arguments) SetQueueMode(mode string) {
	if mode == "" {
		return
	}
	arguments.SetArgs(ArgQueueMode, mode)
}

// SetQueueMasterLocator (主副 线定位器)
func (arguments *Arguments) SetQueueMasterLocator(name string) {
	if name == "" {
		return
	}
	arguments.SetArgs(ArgQueueMasterLocator, name)
}

func (arguments *Arguments) SetQueueLazy() {
	arguments.SetQueueMode(ModeLazy)
}
