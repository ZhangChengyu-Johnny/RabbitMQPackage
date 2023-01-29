package ConsumeMQ

import (
	amqp "github.com/rabbitmq/amqp091-go"
	uuid "github.com/satori/go.uuid"
)

// 发布订阅模式中每个消费者声明一个fanout类型的交换机和一个(随机名称)的队列，使用空的RoutingKey绑定
// 交换机会将消息转发到所有绑定的队列

type subscriptionConsumeMQ struct {
	*basicConsume
	consumeName   string
	ExchangeName  string // 交换机名称
	QueueName     string
	queue         amqp.Queue
	prefetchCount int
	durable       bool
	noWait        bool
}

func NewSubscriptionConsumeMQ(exchangeName string, prefetchCount int, durable, noWait bool) *subscriptionConsumeMQ {
	mq := &subscriptionConsumeMQ{
		basicConsume:  newBasicConsumeMQ("subscription-consume"),
		consumeName:   "subscription-consume-" + uuid.NewV4().String(),
		ExchangeName:  exchangeName,
		prefetchCount: prefetchCount,
		durable:       durable,
		noWait:        noWait,
	}

	// 声明交换机
	mq.exchangeDeclare()

	// 声明队列
	mq.queueDeclare()

	// 绑定交换机和队列
	if err := mq.channel.QueueBind(
		mq.QueueName,    // 随机队列名
		"",              // RoutingKey，该模式用空
		mq.ExchangeName, // 交换机名
		noWait,          // 阻塞
		nil,
	); err != nil {
		mq.failOnError(err, "queue bind exchange failed.")
		panic(err)
	}

	// 配置消费速率
	mq.channel.Qos(prefetchCount, 0, false)
	return mq
}

/* 声明交换机 */
func (mq *subscriptionConsumeMQ) exchangeDeclare() {
	if err := mq.channel.ExchangeDeclare(
		mq.ExchangeName, // 交换机名称
		"fanout",        // 交换机类型，该模式必须fanout
		mq.durable,      // 交换机持久化标记
		false,           // 自动删除
		false,           // 仅rabbitMQ内部使用
		mq.noWait,       // 阻塞
		nil,             // 额外参数
	); err != nil {
		mq.failOnError(err, "declare exchange failed.")
		panic(err)
	}

}

/* 声明队列 */
func (mq *subscriptionConsumeMQ) queueDeclare() {
	var err error
	mq.queue, err = mq.channel.QueueDeclare(
		"", // 随机队列名
		mq.durable,
		false,
		false,
		mq.noWait,
		nil,
	)
	if err != nil {
		mq.failOnError(err, "declare queue failed.")
		panic(err)
	}
	mq.QueueName = mq.queue.Name
}

/* 创建消费者消息管道 */
func (mq *subscriptionConsumeMQ) FanoutChan() (<-chan amqp.Delivery, error) {
	msgChan, err := mq.channel.Consume(
		mq.queue.Name,  // 队列名称
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
	return msgChan, err
}
