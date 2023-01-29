package ConsumeMQ

import (
	"RabbitMQPackage/Config"
	"log"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
	uuid "github.com/satori/go.uuid"
)

// 每个消费者都对应一个队列一个交换机
type basicConsume struct {
	conn             *amqp.Connection
	channel          *amqp.Channel
	dns              string
	Role             string
	consumeName      string     // 消费者名称
	ExchangeName     string     // 交换机名称
	ExchangeType     string     // 交换机类型
	QueueName        string     // 队列名称
	queue            amqp.Queue // 队列
	durable          bool       // 持久化配置
	noWait           bool       // 阻塞配置
	prefetchCount    int        // 消费速率(单次转发多少消息进队列，0为RR模式)
	prefetchSize     int        // 队列消息内存容量
	DeadExchangeName string     // 死信交换机
	DeadQueueName    string     // 死信队列
	DeadRoutingKey   string     // 死信路由
}

func newBasicConsumeMQ(role, exchangeName, queueName string, durable, noWait bool, prefetchCount int, deadQueue bool) *basicConsume {
	var err error
	mq := &basicConsume{
		dns:           Config.MQDNS,
		Role:          role,
		consumeName:   role + "-" + uuid.NewV4().String(),
		ExchangeName:  exchangeName,
		ExchangeType:  strings.Split(role, "-")[0],
		QueueName:     queueName,
		durable:       durable,
		noWait:        noWait,
		prefetchCount: prefetchCount,
		prefetchSize:  0,
	}

	// 建立TCP连接
	mq.conn, err = amqp.Dial(mq.dns)
	if err != nil {
		mq.failOnError(err, "rabbitMQ connection failed.")
		panic(err)
	}

	// 建立信道连接
	mq.channel, err = mq.conn.Channel()
	if err != nil {
		mq.failOnError(err, "make channel failed.")
		panic(err)
	}

	if !deadQueue {
		// 声明交换机
		exchangeType := strings.Split(mq.Role, "-")[0]
		if exchangeType != "default" {
			mq.exchangeDeclare(mq.ExchangeName, exchangeType, mq.durable, mq.noWait)
		}
		// 声明队列
		mq.queue = mq.queueDeclare(mq.QueueName, mq.durable, mq.noWait, nil)

	} else {
		mq.DeadExchangeName = mq.QueueName + "-dead-exchange"
		mq.DeadRoutingKey = mq.QueueName + "-dead-routing"
		mq.DeadQueueName = mq.QueueName + "-dead-queue"
		var deadQueueConf map[string]any = make(map[string]any)

		// 声明普通交换机
		mq.exchangeDeclare(mq.ExchangeName, mq.ExchangeType, mq.durable, mq.noWait)
		// 声明死信交换机
		mq.exchangeDeclare(mq.DeadExchangeName, "direct", mq.durable, mq.noWait)

		// deadQueue绑定死信交换机
		deadQueueConf["x-dead-letter-exchange"] = mq.DeadExchangeName
		// deadQueue的RoutingKey
		deadQueueConf["x-dead-letter-routing-key"] = mq.DeadRoutingKey
		// 配置死信过期时间(一般不用)
		// deadQueueConf["x-message-ttl"] = 1000000

		// 声明普通队列
		mq.queue = mq.queueDeclare(mq.QueueName, mq.durable, mq.noWait, amqp.Table(deadQueueConf))
		// 声明死信队列
		mq.queueDeclare(mq.DeadQueueName, true, false, nil)

		// 绑定死信队列和交换机
		if err := mq.channel.QueueBind(mq.DeadQueueName, mq.DeadRoutingKey, mq.DeadExchangeName, false, nil); err != nil {
			mq.failOnError(err, "bind dead queue and exchange with routingKey failed.")
			panic(err)
		}
	}

	// 配置消费速率
	err = mq.channel.Qos(mq.prefetchCount, mq.prefetchSize, false)
	if err != nil {
		mq.failOnError(err, "Qos failed.")
		panic(err)
	}
	return mq
}

/* 消费者声明队列 */
func (mq *basicConsume) queueDeclare(queueName string, durable, noWait bool, args amqp.Table) amqp.Queue {
	q, err := mq.channel.QueueDeclare(
		queueName, // 队列名称
		durable,   // 队列持久化标记
		false,     // 自动删除
		false,     // 队列独占标记
		noWait,    // 阻塞
		args,      // 额外参数
	)

	if err != nil {
		mq.failOnError(err, "declare queue failed.")
		panic(err)
	}
	return q
}

/* 消费者声明交换机 */
func (mq *basicConsume) exchangeDeclare(exchangeName, exchangeType string, durable, noWait bool) {
	if err := mq.channel.ExchangeDeclare(
		exchangeName, // 交换机名称
		exchangeType, // 交换机类型 [default direct fanout topic]
		durable,      // 交换机持久标记
		false,        // 自动删除
		false,        // 仅rabbitMQ内部使用
		noWait,       // 阻塞
		nil,          // 额外参数
	); err != nil {
		mq.failOnError(err, "declare exchange failed.")
		panic(err)
	}
}

/* 创建消息管道 */
func (mq *basicConsume) messageChan() (<-chan amqp.Delivery, error) {
	// 接收消息
	msgChan, err := mq.channel.Consume(
		mq.QueueName,   // 队列名称
		mq.consumeName, // 消费者名称
		false,          // 自动应答
		false,          // 独占
		false,          // 开启后同一个connection不能传递消息
		mq.noWait,      // 消费队列是否阻塞(msgChan)
		nil,            // 其他参数
	)
	if err != nil {
		mq.failOnError(err, "make message channel failed.")
		return nil, err
	}
	return msgChan, nil
}

func (mq *basicConsume) failOnError(e error, errorMsg string) {
	log.Println("error:", e)
	log.Println("error message:", errorMsg)
}

func (mq *basicConsume) Destory() {
	mq.channel.Close()
	mq.conn.Close()
}

/* 确认消息，MQ收到回复后才删除消息 */
func (mq *basicConsume) Ack(deliveryTag uint64, multiple bool) {
	/*
		deliveryTag: 信道的消息计数器
		multiple: 是否批量确认

		单条确认模式: 单次确认处理完成的消息
		批量确认模式: 批量确认该消息及之前读出的所有消息
	*/
	mq.channel.Ack(deliveryTag, multiple)
}

/* 拒绝消息，包含批量功能 */
func (mq *basicConsume) Nack(deliveryTag uint64, multiple, requeue bool) {
	/*
		deliveryTag: 信道的消息计数器
		multiple: 是否批量拒绝
		requeue: 被拒绝的消息是否重回队列

		单条拒绝模式: 单条拒绝消息
		批量应答模式: 批量拒绝该消息及之前读出的所有消息
		重新入队模式: 被拒绝的消息重新入队，再次分配给消费者
		死信队列模式: 被拒绝的消息加入死信队列，不再重新分配
	*/
	mq.channel.Nack(deliveryTag, multiple, requeue)
}

/* 拒绝消息，仅支持单条拒绝模式 */
func (mq *basicConsume) Reject(deliveryTag uint64, requeue bool) {
	/*
		deliveryTag: 信道的消息计数器
		requeue: 被拒绝的消息是否重回队列

		重新入队模式: 被拒绝的消息重新入队，再次分配给消费者
		死信队列模式: 被拒绝的消息加入死信队列，不再重新分配
	*/
	mq.channel.Reject(deliveryTag, requeue)
}

/* 开启死信队列 */
func (mq *basicConsume) dead() {

}
