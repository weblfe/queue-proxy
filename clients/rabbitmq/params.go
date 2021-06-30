package rabbitmq

import (
	"context"
)

type (

	// QueueParams for mq createQueue 队列参数
	QueueParams struct {
		Name       string                 `json:"name"`
		Durable    bool                   `json:"durable,omitempty"`
		AutoDelete bool                   `json:"autoDelete,omitempty"`
		Exclusive  bool                   `json:"exclusive,omitempty"`
		NoWait     bool                   `json:"noWait,omitempty"`
		Args       map[string]interface{} `json:"args,omitempty"`
	}

	// ConsumerParams for mq createConsumer 参数
	ConsumerParams struct {
		Name      string                 `json:"name"`
		AutoAck   bool                   `json:"autoAck,omitempty"`
		Exclusive bool                   `json:"exclusive,omitempty"`
		NoLocal   bool                   `json:"noLocal,omitempty"`
		NoWait    bool                   `json:"noWait,omitempty"`
		Args      map[string]interface{} `json:"args,omitempty"`
	}

	// PubSubParams
	PubSubParams struct {
		Ctx     context.Context // 上下文
		ConnUrl string          // dns 链接 配置
		Cfg     *BrokerCfg      // 配置
		Entry   string          // env 配置 实例对象命名空间
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
