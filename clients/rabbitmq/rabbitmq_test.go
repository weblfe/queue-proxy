package rabbitmq

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func Test_NewConnection(t *testing.T) {
	_ = os.Setenv("RABBITMQ_DEFAULT_AUTH", "dev:dev")
	var broker = CreateBrokerByEnv("default")
	conn := broker.GetConnection()
	if conn == nil {
		t.Error("conn error")
		return
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Error(err.Error())
	}
	if ch != nil {
		_ = ch.Close()
	}
}

func TestNewPublisher(t *testing.T) {
	_ = os.Setenv("RABBITMQ_DEFAULT_AUTH", "dev:dev")
	var (
		broker         = CreateBrokerByEnv("default")
		client         = NewClient(broker)
		queue          = "test"
		params         = NewSimpleQueueParams(queue)
		pub            = NewSimpleQueuePublisher(client, params)
		consumerParams = NewSimpleConsumerParams("dev.worker", queue)
		sub            = NewSimpleQueueConsumer(NewClientWithEnv("default"), params, consumerParams)
		timer          = time.NewTicker(1 * time.Second)
		times          = 20
		n              = 0
	)
	defer pub.Close()
	defer sub.Close()
	go func() {
		if err := sub.Subscribe(); err != nil {
			t.Error(err)
		}
	}()
	ch := sub.Consume()
	for {
		if n >= times {
			return
		}
		fmt.Println("times:", n)
		select {
		case <-timer.C:
			if err := pub.Send(NewMessage(time.Now().Unix())); err != nil {
				t.Error(err)
				timer.Stop()
				return
			}
		case v := <-ch:
			row := v.GetRowMessage()
			fmt.Println("msg:", string(v.GetContent()))
			fmt.Printf("msg: %v \n", row)
			n++
		}
	}
}

func TestNewPublishPublisher(t *testing.T) {
	_ = os.Setenv("RABBITMQ_DEFAULT_AUTH", "dev:dev")
	var (
		queue1          = "sub1"
		queue2          = "sub2"
		exchange        = "test"
		exchangeType    = ExchangeTypeFanOut
		consumerParams1 = NewSubscribeConsumerParams("sub.worker1", exchange, exchangeType)
		consumerParams2 = NewSubscribeConsumerParams("sub.worker2", exchange, exchangeType)
		params1         = NewSubscribeQueueParams(queue1, exchange, exchangeType)
		params2         = NewSubscribeQueueParams(queue2, exchange, exchangeType)

		sub1 = NewSubscribeConsumer(NewClientWithEnv("default"), params1, consumerParams1, nil)
		sub2 = NewSubscribeConsumer(NewClientWithEnv("default"), params2, consumerParams2, nil)

		pubParams = NewExchangeParams(exchange, exchangeType)
		pub       = NewPublishPublisher(NewClientWithEnv("default"), pubParams)
		timer     = time.NewTicker(1 * time.Second)

		times = 20
		n     = 0
	)
	defer pub.Close()
	defer sub1.Close()
	defer sub2.Close()
	go func() {
		if err := sub1.Subscribe(); err != nil {
			t.Error(err)
		}
	}()
	go func() {
		if err := sub2.Subscribe(); err != nil {
			t.Error(err)
		}
	}()
	ch1 := sub1.Consume()
	ch2 := sub2.Consume()
	for {
		if n >= times {
			return
		}
		fmt.Println("times:", n)
		select {
		case <-timer.C:
			if err := pub.Send(NewMessage(time.Now().Unix())); err != nil {
				t.Error(err)
				timer.Stop()
				return
			}
			n++
		case v := <-ch1:
			row := v.GetRowMessage()
			fmt.Println("msg:", string(v.GetContent()))
			fmt.Printf("msg: %v \n", row)
		case v := <-ch2:
			row := v.GetRowMessage()
			fmt.Println("msg:", string(v.GetContent()))
			fmt.Printf("msg: %v \n", row)
		}
	}

}

func TestNewPublishPublisherAck(t *testing.T) {
	_ = os.Setenv("RABBITMQ_DEFAULT_AUTH", "dev:dev")
	var (
		queue1          = "sub1"
		queue2          = "sub2"
		exchange        = "test"
		exchangeType    = ExchangeTypeFanOut
		consumerParams1 = NewSubscribeConsumerParamsAck("sub.worker1", exchange, exchangeType)
		consumerParams2 = NewSubscribeConsumerParamsAck("sub.worker2", exchange, exchangeType)
		params1         = NewSubscribeQueueParams(queue1, exchange, exchangeType).SetBool("autoDelete", true)
		params2         = NewSubscribeQueueParams(queue2, exchange, exchangeType).SetBool("autoDelete", true)

		sub1 = NewSubscribeConsumer(NewClientWithEnv("default"), params1, consumerParams1, nil)
		sub2 = NewSubscribeConsumer(NewClientWithEnv("default"), params2, consumerParams2, nil)

		pubParams = NewExchangeParams(exchange, exchangeType).SetBool("autoDelete", true)
		pub       = NewPublishPublisher(NewClientWithEnv("default"), pubParams)
		timer     = time.NewTicker(1 * time.Second)

		times = 20
		n     = 0
	)
	defer pub.Close()
	defer sub1.Close()
	defer sub2.Close()
	go func() {
		if err := sub1.Subscribe(); err != nil {
			t.Error(err)
		}
	}()
	go func() {
		if err := sub2.Subscribe(); err != nil {
			t.Error(err)
		}
	}()
	ch1 := sub1.Consume()
	ch2 := sub2.Consume()
	for {
		if n >= times+2 {
			return
		}
		fmt.Println("times:", n)
		select {
		case <-timer.C:
			if n >= times {
				n++
				continue
			}
			if err := pub.Send(NewMessage(time.Now().Unix())); err != nil {
				t.Error(err)
				timer.Stop()
				return
			}
			n++
		case v := <-ch1:
			row := v.GetRowMessage()
			fmt.Println("msg:", string(v.GetContent()))
			fmt.Printf("msg: %v \n", row)
			if err := Ack(v); err != nil {
				t.Error(err)
			}
		case v := <-ch2:
			row := v.GetRowMessage()
			fmt.Println("msg:", string(v.GetContent()))
			fmt.Printf("msg: %v \n", row)
			if err := Ack(v); err != nil {
				t.Error(err)
			}
		}
	}

}
