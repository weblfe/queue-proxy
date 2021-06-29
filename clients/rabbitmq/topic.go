package rabbitmq

type Topic struct {
	chanName  string
	chanType  string
	keyPrefix string
}

type TopicInfo struct {
	ChanName  string `json:"channel_name,omitempty"`
	ChanType  string `json:"channel_type,omitempty"`
	KeyPrefix string `json:"key_prefix,omitempty"`
}

func CreateTopic(name, ty, prefix string) Topic {
	return Topic{chanName: name, chanType: ty, keyPrefix: prefix}
}

func (t *TopicInfo)Create() Topic {
	return CreateTopic(t.ChanName,t.ChanType,t.KeyPrefix)
}

func (topic *Topic) Equal(t Topic) bool {
	return topic.chanName == t.chanName && topic.chanType == t.chanType && topic.keyPrefix == t.keyPrefix
}
