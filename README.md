## 队列服务代理 Queue-Proxy

项目 开发原因 : 各种队列服务 各种协议 各种队列客户端 复杂且不容易学习与维护 

本项目 宗旨在 : 聚合 (常见:kafka,RabbitMQ,beanstalk,nsq) 消息队列系统, 使用redis 协议

> redis 协议依赖库

```go
go get github.com/rfyiamcool/go_pubsub.git 
go get github.com/realpeanut/peanutgredis.git
```

> 队列 依赖库

```go
go get github.com/tal-tech/go-queue
```

> 项目计划

- [ ] v1.0.0 支持 kafka, beanstalk, nsq 消息队列

 - [ ] v1.0.1 支持 redis 协议

 - [ ] v1.0.2 支持 连接池

 - [ ] v1.0.3 支持 RabbitMQ

 - [ ] v1.0.4 支持 持计划化 [ file,mysql, redis, mongodb ]

 - [ ] v1.0.5 支持 nats 

 - [ ] v1.0.6 支持 pulsar

 - [ ] v1.0.7 支持 webUI界面

 - [ ] v1.0.8  支持 clickhouse 存储
