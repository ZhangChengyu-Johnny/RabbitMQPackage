package ConsumeMQ

import (
	amqp "github.com/rabbitmq/amqp091-go"
	uuid "github.com/satori/go.uuid"
)

// 路由模式中的每个消费者声明一个队列和一个direct类型的交换机，使用RoutingKey将两者绑定
// 交换机会根据RoutingKey把消息转发到指定队列

type routingConsumeMQ struct {
	*basicConsume
	consumeName   string
	ExchangeName  string // 交换机名称
	RoutingKey    string // 路由名
	QueueName     string // 队列名
	queue         amqp.Queue
	prefetchCount int
	durable       bool
	noWait        bool
}

func NewRoutingConsumeMQ(exchangeName, queueName, routingKey string, prefetchCount int, durable, noWait bool) *routingConsumeMQ {
	mq := &routingConsumeMQ{
		basicConsume:  newBasicConsumeMQ("routing-consume"),
		consumeName:   "routing-consume-" + uuid.NewV4().String(),
		ExchangeName:  exchangeName,
		RoutingKey:    routingKey,
		QueueName:     queueName,
		prefetchCount: prefetchCount,
		durable:       durable,
		noWait:        noWait,
	}

	// 声明交换机
	mq.exchangeDeclare()

	// 声明队列
	mq.queueDeclare()

	// 队列绑定交换机
	if err := mq.channel.QueueBind(
		mq.QueueName,    // 指定队列名
		mq.RoutingKey,   // 指定路由
		mq.ExchangeName, // 指定交换机
		mq.noWait,       // 阻塞
		nil,             // 额外参数
	); err != nil {
		mq.failOnError(err, "queue bind exchange failed.")
		panic(err)
	}

	// 配置消费速率
	mq.channel.Qos(prefetchCount, 0, false)
	return mq
}

/* 声明交换机 */
func (mq *routingConsumeMQ) exchangeDeclare() {
	if err := mq.channel.ExchangeDeclare(
		mq.ExchangeName, // 交换机名称
		"direct",        // 交换机类型，该模式必须direct
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
func (mq *routingConsumeMQ) queueDeclare() {
	var err error
	mq.queue, err = mq.channel.QueueDeclare(
		mq.QueueName, // 用户指定队列名
		mq.durable,   // 队列持久化标记
		false,        // 自动删除
		false,        // 独占
		mq.noWait,    // 阻塞
		nil,          // 额外参数
	)
	if err != nil {
		mq.failOnError(err, "declare queue failed.")
		panic(err)
	}
}

/* 创建消费者消息管道 */
func (mq *routingConsumeMQ) RoutingChan() (<-chan amqp.Delivery, error) {
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
