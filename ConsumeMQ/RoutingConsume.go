package ConsumeMQ

import (
	amqp "github.com/rabbitmq/amqp091-go"
	uuid "github.com/satori/go.uuid"
)

type RoutingConsumeMQ struct {
	*basicConsume
	ExchangeName  string // 交换机名称
	RoutingKey    string // 路由名
	QueueName     string // 队列名
	queue         amqp.Queue
	prefetchCount int
}

func NewRoutingConsumeMQ(exchangeName, queueName, routingKey string, prefetchCount int, durable, noWait bool, args amqp.Table) *RoutingConsumeMQ {
	mq := &RoutingConsumeMQ{
		basicConsume: newBasicConsumeMQ("routing-consume"),
		ExchangeName: exchangeName,
		RoutingKey:   routingKey,
		QueueName:    queueName,
	}
	// 声明交换机
	mq.exchangeDeclare(durable, noWait, args)
	// 声明队列
	mq.queueDeclare(queueName, durable, noWait, args)
	// 队列绑定交换机
	if err := mq.channel.QueueBind(
		mq.QueueName,    // 指定队列名
		mq.RoutingKey,   // 指定路由
		mq.ExchangeName, // 指定交换机
		noWait,          // 阻塞
		args,
	); err != nil {
		mq.failOnError(err, "queue bind exchange failed.")
		panic(err)
	}

	// 配置消费速率
	mq.channel.Qos(prefetchCount, 0, false)
	mq.prefetchCount = prefetchCount
	return mq
}

/* 声明交换机 */
func (mq *RoutingConsumeMQ) exchangeDeclare(durable, noWait bool, args amqp.Table) {
	if err := mq.channel.ExchangeDeclare(
		mq.ExchangeName, // 交换机名称
		"direct",        // 交换机类型，该模式必须direct
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
func (mq *RoutingConsumeMQ) queueDeclare(queueName string, durable, noWait bool, args amqp.Table) {
	var err error
	mq.queue, err = mq.channel.QueueDeclare(
		queueName,
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
func (mq *RoutingConsumeMQ) RoutingChan(noWait bool, args amqp.Table) (msgChan <-chan amqp.Delivery, err error) {
	// 接收消息
	msgChan, err = mq.channel.Consume(
		mq.QueueName,                  // 队列名称
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
