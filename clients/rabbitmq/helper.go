package rabbitmq

import (
	"encoding/json"
	"fmt"
	"github.com/streadway/amqp"
	"os"
	"runtime"
	"strconv"
	"time"
)

// GetByEnvOf 通过env 获取 string 值
func GetByEnvOf(key string, def ...string) string {
	def = append(def, "")
	var v = os.Getenv(key)
	if v != "" {
		return v
	}
	return def[0]
}

// GetBoolByEnvOf 通过env 获取 bool 值
func GetBoolByEnvOf(key string, def ...bool) bool {
	def = append(def, false)
	var v = os.Getenv(key)
	if v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
		switch v {
		case "yes":
			return true
		case "Yes":
			return true
		case "YES":
			return true
		case "ON":
			return true
		case "on":
			return true
		case "On":
			return true
		case "1":
			return true
		}
	}
	return def[0]
}

// GetDurationByEnvOf 通过env 获取 duration 值
func GetDurationByEnvOf(key string, def ...time.Duration) time.Duration {
	def = append(def, 0)
	var v = os.Getenv(key)
	if v != "" {
		if b, err := time.ParseDuration(v); err == nil {
			return b
		}
		if n, err := strconv.Atoi(v); err == nil {
			return time.Duration(n)
		}
	}
	return 0
}

// GetJsonByEnvBind 通过env 获取 object 值
func GetJsonByEnvBind(key string, v interface{}) error {
	if v == nil {
		_, file, line, _ := runtime.Caller(0)
		return fmt.Errorf("%s, at line : %d ,%s", file, line, "GetJsonByEnvBind.Error v is Nil")
	}
	var data = os.Getenv(key)
	if data == "" {
		return fmt.Errorf("Env.Nil")
	}
	if err := json.Unmarshal([]byte(data), v); err != nil {
		return err
	}
	return nil
}

// Ack 对所标识的消息的传递予以确认 [消费成功回复]
func Ack(v MessageWrapper, multiple ...bool) error {
	var msg = v.GetRowMessage()
	multiple = append(multiple, false)
	switch msg.(type) {
	case amqp.Delivery:
		m := msg.(amqp.Delivery)
		return m.Ack(multiple[0])
	case MessageReplier:
		m := msg.(MessageReplier)
		return m.Ack(multiple[0])
	}
	return fmt.Errorf("[RABBITMQ_ACK] Type Error: %v,Must Be MessageReplier Interface Type", msg)
}

// Reject 对所标识的消息的传递予以拒绝 [拒绝消费]
func Reject(v MessageWrapper, requeue ...bool) error {
	var msg = v.GetRowMessage()
	requeue = append(requeue, false)
	switch msg.(type) {
	case amqp.Delivery:
		m := msg.(amqp.Delivery)
		return m.Reject(requeue[0])
	case MessageReplier:
		m := msg.(MessageReplier)
		return m.Reject(requeue[0])
	}
	return fmt.Errorf("[RABBITMQ_REJECT] Type Error: %v,Must Be MessageReplier Interface Type", msg)
}

// Nack 对所标识的消息的传递予以否定 [消费失败]
func Nack(v MessageWrapper, args ...bool) error {
	var msg = v.GetRowMessage()
	args = append(args, false)
	if len(args) < 2 {
		args = append(args, false)
	}
	switch msg.(type) {
	case amqp.Delivery:
		m := msg.(amqp.Delivery)
		return m.Nack(args[0], args[1])
	case MessageReplier:
		m := msg.(MessageReplier)
		return m.Nack(args[0], args[1])
	}
	return fmt.Errorf("[RABBITMQ_NACK] Type Error: %v,Must Be MessageReplier Interface Type", msg)
}

// 创建 Publishing
func CreatePublishing(data interface{}) amqp.Publishing {
	var msg = amqp.Publishing{
		Body:            nil,
		Expiration:      defaultMsgTtl,
		DeliveryMode:    amqp.Persistent, // 默认持久化
		ContentEncoding: defaultContentEncode,
		ContentType:     defaultContentType,
		Timestamp:       time.Now(),
	}
	if data == nil {
		return msg
	}
	switch data.(type) {
	case amqp.Publishing:
		msg = data.(amqp.Publishing)
	case *amqp.Publishing:
		msg = *data.(*amqp.Publishing)
	case amqp.Delivery:
		m := data.(amqp.Delivery)
		msg = DeliveryToPublishing(m)
	case *amqp.Delivery:
		m := data.(*amqp.Delivery)
		msg = DeliveryToPublishing(*m)
	case string:
		msg.Body = []byte(data.(string))
	case []byte:
		msg.Body = data.([]byte)
	case fmt.Stringer:
		msg.Body = []byte(data.(fmt.Stringer).String())
	default:
		m, err := json.Marshal(data)
		if err != nil {
			return msg
		}
		msg.Body = m
	}
	return msg
}

// 创建 Message
func messageParamsFor(data interface{}) *MessageParams {
	if data == nil {
		return nil
	}
	switch data.(type) {
	case MessageParams:
		d := data.(MessageParams)
		return &d
	case *MessageParams:
		return data.(*MessageParams)
	case string:
		var msg = &MessageParams{}
		if err := json.Unmarshal([]byte(data.(string)), msg); err == nil {
			return msg
		}
	case []byte:
		var msg = &MessageParams{}
		if err := json.Unmarshal(data.([]byte), msg); err == nil {
			return msg
		}
	case amqp.Publishing:
		return &MessageParams{
			Msg: data.(amqp.Publishing),
		}
	case *amqp.Publishing:
		return &MessageParams{
			Msg: *data.(*amqp.Publishing),
		}
	case amqp.Delivery:
		var d = data.(amqp.Delivery)
		return &MessageParams{
			Msg: DeliveryToPublishing(d),
		}
	case *amqp.Delivery:
		var d = *data.(*amqp.Delivery)
		return &MessageParams{
			Msg: DeliveryToPublishing(d),
		}
	}
	return nil
}

func DeliveryToPublishing(d amqp.Delivery) amqp.Publishing {
	return amqp.Publishing{
		Body:            d.Body,
		Type:            d.Type,
		Expiration:      d.Expiration,
		Priority:        d.Priority,
		ContentEncoding: d.ContentEncoding,
		ContentType:     d.ContentType,
		DeliveryMode:    d.DeliveryMode,
		Headers:         d.Headers,
		MessageId:       d.MessageId,
		AppId:           d.AppId,
		UserId:          d.UserId,
		Timestamp:       time.Now(),
	}
}

// CheckTypeArray 检查是否交换机 支持的类型
func CheckTypeArray(t string) bool {
	for _, v := range []string{ExchangeTypeDirect, ExchangeTypeFanOut, ExchangeTypeTopic, ExchangeTypeHeader} {
		if v == t {
			return true
		}
	}
	return false
}

// 数组去重
func ArrayUnique(arr []string) (newArr []string) {
	newArr = make([]string, 0)
	for i := 0; i < len(arr); i++ {
		repeat := false
		for j := i + 1; j < len(arr); j++ {
			if arr[i] == arr[j] {
				repeat = true
				break
			}
		}
		if !repeat {
			newArr = append(newArr, arr[i])
		}
	}
	return
}

// 获取AMQP消息
func MessageForDelivery(msg MessageWrapper) *amqp.Delivery {
	var data = msg.GetRowMessage()
	switch data.(type) {
	case amqp.Delivery:
		var d = data.(amqp.Delivery)
		return &d
	case *amqp.Delivery:
		return data.(*amqp.Delivery)
	}
	return nil
}

func GetBroker(p Publisher) *Broker {
	switch p.(type) {
	case *SimpleQueuePublisher:
		return p.(*SimpleQueuePublisher).GetBroker()
	case *PublishPublisher:
		return p.(*PublishPublisher).GetBroker()
	case *RoutingPublisher:
		return p.(*RoutingPublisher).GetBroker()
	case *TopicPublisher:
		return p.(*TopicPublisher).GetBroker()
	}
	return nil
}
