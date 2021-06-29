package rabbitmq

import (
	"encoding/json"
	"github.com/streadway/amqp"
)

type Topic struct {
	chanName                              string
	chanType                              string
	keyPrefix                             string
	durable, autoDelete, internal, noWait bool
	args                                  map[string]interface{}
}

type TopicInfo struct {
	ChanName   string                 `json:"channel_name,omitempty"`
	ChanType   string                 `json:"channel_type,omitempty"`
	KeyPrefix  string                 `json:"key_prefix,omitempty"`
	Durable    bool                   `json:"durable,default=false"`
	AutoDelete bool                   `json:"auto_delete,default=false"`
	Internal   bool                   `json:"internal,default=false"`
	NoWait     bool                   `json:"no_wait,default=false"`
	Args       map[string]interface{} `json:"args,omitempty"`
}

func CreateTopic(name, ty, prefix string) Topic {
	return Topic{chanName: name, chanType: ty, keyPrefix: prefix, args: nil}
}

func (t *TopicInfo) Create() Topic {
	var tp = CreateTopic(t.ChanName, t.ChanType, t.KeyPrefix)
	tp.autoDelete = t.AutoDelete
	tp.internal = t.Internal
	tp.noWait = t.NoWait
	tp.args = t.Args
	return tp
}

func (topic *Topic) Equal(t Topic) bool {
	return topic.chanName == t.chanName && topic.chanType == t.chanType && topic.keyPrefix == t.keyPrefix
}

func (topic *Topic) GetArgs() amqp.Table {
	return  topic.args
}

func (topic *Topic) String() string {
	var s, err = json.Marshal(topic.ToInfo())
	if err != nil {
		return ""
	}
	return string(s)
}

func (topic *Topic) ToInfo() TopicInfo {
	return TopicInfo{
		ChanName:   topic.chanName,
		ChanType:   topic.chanType,
		KeyPrefix:  topic.keyPrefix,
		Durable:    topic.durable,
		AutoDelete: topic.autoDelete,
		Internal:   topic.internal,
		NoWait:     topic.noWait,
		Args:       topic.args,
	}
}
