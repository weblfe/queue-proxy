package config

// QueueDriver 队列驱动
type QueueDriver string

const (
	DriverQNil        QueueDriver = ""
	DriverKafka     QueueDriver = "kafka"
	DriverBeanstalk QueueDriver = "beanstalk"
	DriverNsq       QueueDriver = "nsq"
	DriverNats      QueueDriver = "nats"
	DriverRabbitmq  QueueDriver = "rabbitmq"
)
