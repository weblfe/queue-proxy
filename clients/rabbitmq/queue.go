package rabbitmq

import "context"

// NewPublisher init a Publisher of rabbitmq client
func NewPublisher(params PubSubParams) *Client {
	var (
		clt = new(Client)
	)
	clt.ctx, clt.cancel = context.WithCancel(params.GetContext())
	return clt.BindBroker(params.GetBrokerCfg().createBroker())
}

// NewConsumer init a Consumer of rabbitmq client
func NewConsumer(params PubSubParams) *Client {
	var (
		clt = new(Client)
	)
	clt.ctx, clt.cancel = context.WithCancel(params.GetContext())
	return clt.BindBroker(params.GetBrokerCfg().createBroker())
}

