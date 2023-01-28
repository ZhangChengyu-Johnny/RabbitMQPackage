package ConsumeMQ

import (
	amqp "github.com/rabbitmq/amqp091-go"
	uuid "github.com/satori/go.uuid"
)

/* 发布订阅模式下，一个消费者对应一个队列，多个消费者队列绑定一个交换机 */
type SubscriptionConsumeMQ struct {
	*basicConsume
	ExchangeName  string // 交换机名称
	queue         amqp.Queue
	prefetchCount int
}

func NewSubscriptionConsumeMQ(exchangeName string, prefetchCount int, durable, noWait bool, args amqp.Table) *SubscriptionConsumeMQ {
	mq := &SubscriptionConsumeMQ{
		basicConsume: newBasicConsumeMQ("subscription-publish"),
		ExchangeName: exchangeName,
	}
	// 发布订阅模式实例化生产者时需要声明交换机
	mq.exchangeDeclare(durable, noWait, args)
	// 声明队列
	mq.queueDeclare(durable, noWait, args)
	// 绑定交换机和队列
	if err := mq.channel.QueueBind(
		mq.queue.Name,   // 随机队列名
		"",              // RoutingKey，该模式用空
		mq.ExchangeName, // 交换机名
		noWait,          // 阻塞
		args,
	); err != nil {
		mq.failOnError(err, "queue bind exchange failed.")
		panic(err)
	}

	mq.channel.Qos(prefetchCount, 0, false)
	mq.prefetchCount = prefetchCount
	return mq
}

/* 声明交换机 */
func (mq *SubscriptionConsumeMQ) exchangeDeclare(durable, noWait bool, args amqp.Table) {
	if err := mq.channel.ExchangeDeclare(
		mq.ExchangeName, // 交换机名称
		"fanout",        // 交换机类型
		durable,         // 交换机持久化标记
		false,           // 自动删除
		false,           // 仅rabbitMQ内部使用
		noWait,          // 阻塞
		args,            // 额外参数
	); err != nil {
		mq.failOnError(err, "declare exchange failed.")
		panic(err)
	}

}

/* 声明队列 */
func (mq *SubscriptionConsumeMQ) queueDeclare(durable, noWait bool, args amqp.Table) {
	var err error
	mq.queue, err = mq.channel.QueueDeclare(
		"", // 随机队列名
		durable,
		false,
		false,
		noWait,
		args,
	)
	if err != nil {
		mq.failOnError(err, "declare queue failed.")
		panic(err)
	}
}

/* 创建消费者消息管道 */
func (mq *SubscriptionConsumeMQ) SubscriptionMessageChan(noWait bool, args amqp.Table) (msgChan <-chan amqp.Delivery, err error) {
	// 接收消息
	msgChan, err = mq.channel.Consume(
		mq.queue.Name,                 // 队列名称
		mq.Role+uuid.NewV4().String(), // 消费者名称
		false,                         // 自动应答
		false,                         // 独占
		false,                         // 开启后同一个connection不能传递消息
		noWait,                        // 消费队列是否阻塞(msgChan)
		args,                          // 其他参数
	)
	if err != nil {
		mq.failOnError(err, "make message channel failed.")
		return
	}
	return
}
