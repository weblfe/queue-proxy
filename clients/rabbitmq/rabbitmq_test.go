package rabbitmq

import (
	"fmt"
	"log"
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
	defer errorLog(pub.Close)
	defer errorLog(sub.Close)
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

func TestNewPublisherQos(t *testing.T) {
	_ = os.Setenv("RABBITMQ_DEFAULT_AUTH", "dev:dev")
	var (
		broker          = CreateBrokerByEnv("default")
		client          = NewClient(broker)
		queue           = "worker"
		params1         = NewSimpleQueueParams(queue)
		pub             = NewSimpleQueuePublisher(client, params1)
		consumerParams1 = NewSimpleConsumerParams("dev.worker1", queue)
		consumerParams2 = NewSimpleConsumerParams("dev.worker2", queue)
		sub1            = NewSimpleQueueConsumer(NewClientWithEnv("default"), params1, consumerParams1)
		sub2            = NewSimpleQueueConsumer(NewClientWithEnv("default"), params1, consumerParams2)
		timer           = time.NewTicker(1 * time.Second)
		times           = 20
		n               = 0
	)
	sub1.SetQos(true)
	sub2.SetQos(true)
	defer errorLog(pub.Close)
	defer errorLog(sub1.Close)
	defer errorLog(sub2.Close)
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
		case v := <-ch1:
			row := v.GetRowMessage()
			fmt.Println("msg:", string(v.GetContent()))
			fmt.Printf("msg: %v \n", row)
			n++
		case v := <-ch2:
			row := v.GetRowMessage()
			fmt.Println("msg:", string(v.GetContent()))
			fmt.Printf("msg: %v \n", row)
			n++
		}
	}
}

func TestNewPublisherQosNot(t *testing.T) {
	_ = os.Setenv("RABBITMQ_DEFAULT_AUTH", "dev:dev")
	var (
		broker          = CreateBrokerByEnv("default")
		client          = NewClient(broker)
		queue           = "worker"
		params          = NewSimpleQueueParams(queue).SetBool("autoDelete", true)
		pub             = NewSimpleQueuePublisher(client, params)
		consumerParams1 = NewSimpleConsumerParams("dev.worker1", queue).SetBool("autoDelete", true)
		consumerParams2 = NewSimpleConsumerParams("dev.worker2", queue).SetBool("autoDelete", true)

		timer = time.NewTicker(1 * time.Second)
		times = 20
		n     = 0
	)
	consumerParams2.SetMaxPriority(100)
	consumerParams1.SetMaxPriority(10)
	sub1 := NewSimpleQueueConsumer(NewClientWithEnv("default"), params, consumerParams1)
	sub2 := NewSimpleQueueConsumer(NewClientWithEnv("default"), params, consumerParams2)
	sub1.SetQos(false)
	sub2.SetQos(false)
	defer errorLog(pub.Close)
	defer errorLog(sub1.Close)
	defer errorLog(sub2.Close)
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
		case v := <-ch1:
			row := v.GetRowMessage()
			fmt.Println("msg:", string(v.GetContent()))
			fmt.Printf("msg: %v \n", row)
			n++
		case v := <-ch2:
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
	defer errorLog(pub.Close)
	defer errorLog(sub1.Close)
	defer errorLog(sub2.Close)
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
	if err := pub.DeleteExchange(); err != nil {
		t.Error(err)
	}
	if err := sub1.Delete(); err != nil {
		t.Error(err)
	}
	if err := sub2.Delete(); err != nil {
		t.Error(err)
	}
	defer errorLog(pub.Close)
	defer errorLog(sub1.Close)
	defer errorLog(sub2.Close)
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
			if err := pub.Send(MessageParamsCreate(time.Now().Unix())); err != nil {
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

func TestNewRoutingPublisherAck(t *testing.T) {
	_ = os.Setenv("RABBITMQ_DEFAULT_AUTH", "dev:dev")
	var (
		queue1          = "r1"
		queue2          = "r2"
		exchange        = "routing"
		key1            = "info"
		key2            = "error"
		consumerParams1 = NewRoutingConsumerParamsAck("info.worker1", exchange, key1)
		consumerParams2 = NewRoutingConsumerParamsAck("error.worker1", exchange, key2)
		params1         = NewRoutingQueueParams(queue1, exchange, key1).SetBool("autoDelete", true)
		params2         = NewRoutingQueueParams(queue2, exchange, key2).SetBool("autoDelete", true)

		sub1 = NewSubscribeConsumer(NewClientWithEnv("default"), params1, consumerParams1, nil)
		sub2 = NewSubscribeConsumer(NewClientWithEnv("default"), params2, consumerParams2, nil)

		pubParams = NewRoutingExchangeParams(exchange).SetBool("autoDelete", true)
		pub       = NewRoutingPublisher(NewClientWithEnv("default"), pubParams)
		timer     = time.NewTicker(1 * time.Second)

		times = 50
		n     = 0
	)
	if err := pub.DeleteExchange(); err != nil {
		t.Error(err)
	}
	if err := sub1.Delete(); err != nil {
		t.Error(err)
	}
	if err := sub2.Delete(); err != nil {
		t.Error(err)
	}
	defer errorLog(pub.Close)
	defer errorLog(sub1.Close)
	defer errorLog(sub2.Close)
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
			key := key1
			// 随机更换 key
			/*if rand.Int() % 2 > 0 {
				key = key2
			}*/
			// 前一半 key1, 后一半 key2
			if n > times/2 {
				key = key2
			}
			if err := pub.Send(MessageParamsCreate(time.Now().Unix()).SetKey(key)); err != nil {
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

func TestNewTopicPublisherAck(t *testing.T) {
	_ = os.Setenv("RABBITMQ_DEFAULT_AUTH", "dev:dev")
	var (
		queue1   = "topic1"
		queue2   = "topic2"
		exchange = "topic"
		// <speed>.<colour>.<species>
		key1            = "*.yellow.*,1.#"
		key2            = "*.*.rabbit,*.*.*"
		pubKey          = "1.yellow.rabbit"
		consumerParams1 = NewTopicConsumerParamsAck("info.worker1", exchange, key1)
		consumerParams2 = NewTopicConsumerParamsAck("error.worker1", exchange, key2)
		params1         = NewTopicQueueParams(queue1, exchange, key1).SetBool("autoDelete", true)
		params2         = NewTopicQueueParams(queue2, exchange, key2).SetBool("autoDelete", true)

		sub1 = NewTopicConsumer(NewClientWithEnv("default"), params1, consumerParams1, nil)
		sub2 = NewTopicConsumer(NewClientWithEnv("default"), params2, consumerParams2, nil)

		pubParams = NewTopicExchangeParams(exchange).SetBool("autoDelete", true)
		pub       = NewTopicPublisher(NewClientWithEnv("default"), pubParams)
		timer     = time.NewTicker(1 * time.Second)

		times = 50
		n     = 0
	)
	defer errorLog(pub.Close)
	defer errorLog(sub1.Close)
	defer errorLog(sub2.Close)
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
			if err := pub.Send(MessageParamsCreate(time.Now().Unix()).SetKey(pubKey)); err != nil {
				t.Error(err)
				timer.Stop()
				return
			}
			n++
		case v := <-ch1:
			row := v.GetRowMessage()
			fmt.Println("topic key: ", sub1.GetTopicKeys())
			fmt.Println("routing key", MessageForDelivery(v).RoutingKey)
			fmt.Println("msg:", string(v.GetContent()))
			fmt.Printf("msg: %v \n", row)
			if err := Ack(v); err != nil {
				t.Error(err)
			}
		case v := <-ch2:
			row := v.GetRowMessage()
			fmt.Println("topic key: ", sub2.GetTopicKeys())
			fmt.Println("routing key", MessageForDelivery(v).RoutingKey)
			fmt.Println("msg:", string(v.GetContent()))
			fmt.Printf("msg: %v \n", row)
			if err := Ack(v); err != nil {
				t.Error(err)
			}
		}
	}

}

func errorLog(fn func() error) {
	if err := fn(); err != nil {
		log.Println(err.Error())
	}
}
