package rabbitmq

// durable: 是否持久化, 队列的声明默认是存放到内存中的，
// 如果rabbitmq重启会丢失，如果想重启之后还存在就要使队列持久化，
// 保存到Erlang自带的Mnesia数据库中，当rabbitmq重启之后会读取该数据库

// exclusive：是否排外的，有两个作用，
// 一：当连接关闭时connection.close()该队列是否会自动删除；
// 二：该队列是否是私有的private，如果不是排外的，可以使用两个消费者都访问同一个队列，没有任何问题，如果是排外的，会对当前队列加锁，
// 其他通道channel是不能访问的，如果强制访问会报异常

// autoDelete：是否自动删除，当最后一个消费者断开连接之后队列是否自动被删除，
//  可以通过RabbitMQ Management，查看某个队列的消费者数量，当consumers = 0时队列就会自动删除

// exchange 交换机名
//
//routingKey 路由键 (tag) (255字节)
//
//mandatory 强制性
//    建议false当mandatory标志位设置为true时，如果exchange根据自身类型和消息routingKey无法找到一个合适的queue存储消息，
//   那么broker会调用basic.return方法将消息返还给生产者;当mandatory设置为false时，出现上述情况broker会直接将消息丢弃;
//   通俗的讲，mandatory标志告诉broker代理服务器至少将消息route到一个队列中，否则就将消息return给发送者;
//
//immediate 消息
// 建议false当immediate标志位设置为true时，
// 如果exchange在将消息路由到queue(s)时发现对于的queue上么有消费者，
// 那么这条消息不会放入队列中。当与消息routeKey关联的所有queue(一个或者多个)都没有消费者时，
// 该消息会通过basic.return方法返还给生产者。
//
//publishing 消息
//DeliveryMode: amqp.Persistent 设置消息持久化


// 生产者丢失消息解决方案 推荐使用confirm模式
// 从生产者弄丢数据这个角度来看，RabbitMQ提供transaction和confirm模式来确保生产者不丢消息。
// transaction机制发送消息前，开启事务(channel.txSelect()),然后发送消息，如果发送过程中出现什么异常，
//  事务就会回滚(channel.txRollback()),如果发送成功则提交事务(channel.txCommit())。缺点：吞吐量下降。

// confirm模式一旦channel进入confirm模式，所有在该信道上发布的消息都将会被指派一个唯一的ID(从1开始)，
// 一旦消息被投递到所有匹配的队列之后；rabbitMQ就会发送一个ACK给生产者(包含消息的唯一ID)，
//这就使得生产者知道消息已经正确到达目的队列了；如果rabbitMQ没能处理该消息，则会发送一个Nack消息给你，你可以进行重试操作。

// 声明交换机
// name 交换机名
//kind 类型
//fanout(广播)
//direct(直接交换)比fanout多加了一层密码限制(routingKey)
//topic(主题)
//headers(首部)

//durable 是否持久化 建议true是否持久化，RabbitMQ关闭后，没有持久化的Exchange将被清除
//autoDelete 是否自动删除 建议false是否自动删除，如果没有与之绑定的Queue，直接删除
//internal 是否内置的 建议false非内置的，如果为true，只能通过Exchange到Exchange
//noWait 是否非阻塞 建议false
//true表示是。
//阻塞：表示创建交换器的请求发送后，阻塞等待RMQ Server返回信息。
//非阻塞：不会阻塞等待RMQ Server的返回信息，而RMQ Server也不会返回信息。(不推荐使用)


// **调用模式**

// 1. 简单队列 (1 对 1 ) [Queue - Worker ]

// 2. 工作队列 (1 对 多) [Queue + Workers]
// 2.1)循环调度
//  RabbitMQ会按顺序得把消息发送给每个消费者(consumer)。
//   平均每个消费者都会收到同等数量得消息。这种发送消息得方式叫做——轮询(round-robin)。
//   但是毕竟每个消费者的消费能力不一致，会造成部分消费者”吃的很撑“，部分消费者“还很饥饿”，所以通常该模式并不是很提倡。
// 2.2) 公平调度
// 设置预取计数值为1。告诉RabbitMQ一次只向一个worker发送一条消息。公平分发，某消费者在工作饱和情况下不发送消息给该消费者。
//  err = ch.Qos(
//   1,     // prefetch count
//   0,     // prefetch size
//   false, // global
//)

// 3. 发布订阅 [Exchange<fanout> + Queues(1-N)<"">{Queue-Worker} ]
// err = ch.QueueBind(
//  q.Name, // queue name
//  "",     // routing key
//  "your_exchange", // exchange
//  false,
//  nil,
//)

// 4.路由模式 [ Exchange<direct> + Queues(1-N)<Routing-Key>{Queue-Worker}]
// 设置routing key
//
// err = ch.QueueBind(
//  q.Name, // queue name
//  "info",     // routing key 带对应routing key 转发到对应 绑定队列
//  "your_exchange", // exchange
//  false,
//  nil,
//)

// 5.主题模式  [ Exchange<topic> + Queues(1-N)<Pattern-Routing-Key:[*|#]>{Queue-Worker}]
// * (星号) 用来表示一个单词.
// # (井号) 用来表示任意数量(零个或多个)单词。

// 6. 远程调用
// 当客户端启动的时候，它创建一个匿名独享的回调队列。
// 在RPC中，客户端发送带有两个属性的消息：一个是设置回调队列的 reply_to 属性，另一个是设置唯一值的 correlation_id 属性。
// 将请求发送到一个 rpc_queue 队列中。
// RPC工作者(又名：服务器)等待请求发送到这个队列中来。当请求出现的时候，它执行他的工作并且将带有执行结果的消息发送给reply_to字段指定的队列。
// 客户端等待回调队列里的数据。当有消息出现的时候，它会检查correlation_id属性。如果此属性的值与请求匹配，将它返回给应用。
