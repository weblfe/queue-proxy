package rabbitmq

import (
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"log"
)

type RpcService struct {
	caller    Publisher                                  // rpc 调用队列
	callback  Consumer                                   // 当前进程 私有队列
	onSuccess func(rpc *RpcService, data MessageWrapper) // 成功回调
	onError   func(rpc *RpcService, data MessageWrapper) // 失败回调
	service   func(data *amqp.Delivery) amqp.Publishing  // 服务
	request   map[string]interface{}                     // 请求记录
	error     map[string]interface{}                     // 失败记录
	ch        chan bool                                  // 控制通道
	replyTo   string                                     // 匿名队列名
}

const (
	defaultErrorSize = 100
)

func NewRpcQueueService(caller Publisher, callback Consumer) *RpcService {
	return &RpcService{
		caller:   caller,
		callback: callback,
		request:  make(map[string]interface{}, 10),
		error:    make(map[string]interface{}, 10),
	}
}

func (rpc *RpcService) init() {
	if rpc.onSuccess == nil {
		rpc.onSuccess = func(rpc *RpcService, data MessageWrapper) { fmt.Println("OnSuccess", data) }
	}
	if rpc.onError == nil {
		rpc.onError = func(rpc *RpcService, data MessageWrapper) { fmt.Println("onError", data) }
	}
}

func (rpc *RpcService) SetReplyTo(replyTo string) *RpcService {
	rpc.replyTo = replyTo
	return rpc
}

func (rpc *RpcService) OnSuccess(handler func(rpc *RpcService, data MessageWrapper)) *RpcService {
	if rpc.onSuccess == nil {
		rpc.onSuccess = handler
	}
	return rpc
}

func (rpc *RpcService) OnError(handler func(rpc *RpcService, data MessageWrapper)) *RpcService {
	if rpc.onError == nil {
		rpc.onError = handler
	}
	return rpc
}

func (rpc *RpcService) GetErrors() map[string]interface{} {
	return rpc.error
}

func (rpc *RpcService) Call(params interface{}, requestId string) error {
	if err := rpc.caller.Send(messageParamsFor(params).SetReplyTo(rpc.replyTo).SetCorrelationId(requestId)); err != nil {
		return err
	}
	rpc.request[requestId] = nil
	return nil
}

func (rpc *RpcService) run() {
	rpc.init()
	for {
		select {
		case v := <-rpc.callback.Consume():
			resp := MessageForDelivery(v)
			if resp == nil {
				rpc.onError(rpc, v)
				continue
			}
			if resp.CorrelationId == "" {
				rpc.onError(rpc, v)
				continue
			}
			if resp.Body == nil {
				rpc.onError(rpc, v)
				rpc.SaveError(resp.CorrelationId, errors.New("empty response"))
				continue
			}
			rpc.onSuccess(rpc, v)
			delete(rpc.request, resp.CorrelationId)
		case <-rpc.ch:
			rpc.error = nil
			rpc.request = nil
			return

		}
	}
}

func (rpc *RpcService) ReplyTo(service *RpcService, data MessageWrapper) error {
	var msg = MessageForDelivery(data)
	broker := GetBroker(service.caller)
	if broker == nil {
		return errors.New("[RpcService.GetBroker] Error Nil")
	}
	replyTo := msg.ReplyTo
	if replyTo == "" {
		if err := Ack(data); err != nil {
			return err
		}
		return nil
	}
	connection, err := broker.GetConnector()
	if err != nil {
		_ = Nack(data)
		return err
	}
	// @todo 优化每次请不重新发起链接
	defer connection.Close()
	// 获取信道
	ch, err := connection.Channel()
	if err != nil {
		_ = Nack(data)
		return err
	}
	// 定义队列
	_, err = ch.QueueDeclare(replyTo, true, false, false, false, nil)
	if err != nil {
		_ = Nack(data)
		return err
	}
	// 只回复到指定队列
	err = ch.Publish("", replyTo, false, false, rpc.Service(msg))
	if err != nil {
		_ = Nack(data)
		return err
	}
	if err = Ack(data); err != nil {
		return err
	}
	return nil
}

// 调用绑定服务
func (rpc *RpcService) Service(params *amqp.Delivery) amqp.Publishing {
	if rpc.service == nil {
		return DeliveryToPublishing(*params)
	}
	return rpc.service(params)
}

// 注册服务
func (rpc *RpcService) RegisterService(service func(params *amqp.Delivery) amqp.Publishing) *RpcService {
	if rpc.service == nil {
		rpc.service = service
	}
	return rpc
}

func (rpc *RpcService) SaveError(id string, v interface{}) {
	if len(rpc.error) >= defaultErrorSize {
		for k, err := range rpc.error {
			log.Printf("[RpcService.Error'],requestId:%s error:%v \n", k, err)
			delete(rpc.error, k)
		}
	}
	rpc.error[id] = v
	delete(rpc.request, id)
}

func (rpc *RpcService) ListenAndServe() error {
	// 运行监听
	go rpc.run()
	// 开始定义结果
	return rpc.callback.Subscribe()
}

func (rpc *RpcService) Close() error {
	defer func() {
		if err := rpc.caller.Close(); err != nil {
			log.Println("[RpcService.caller.Close]Error", err.Error())
		}
		rpc.ch <- true
	}()
	return rpc.callback.Close()
}
