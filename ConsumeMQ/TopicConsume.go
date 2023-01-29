package ConsumeMQ

import (
	amqp "github.com/rabbitmq/amqp091-go"
	uuid "github.com/satori/go.uuid"
)

// 主体模式中每个消费者声明一个topics类型的交换机和一个队列，该队列可以绑定多个RoutingKey
// 交换机会将消息转发到所有命中匹配规则的队列

type topicConsumeMQ struct {
	*basicConsume
	consumeName   string
	ExchangeName  string
	QueueName     string
	queue         amqp.Queue
	RoutingKeys   []string
	prefetchCount int
	durable       bool
	noWait        bool
}

func NewTopicConsumeMQ(exchangeName, queueName string, routingKeys []string,
	prefetchCount int, durable, noWait bool) *topicConsumeMQ {
	mq := &topicConsumeMQ{
		basicConsume:  newBasicConsumeMQ("topic-consumer"),
		consumeName:   "topic-consumer-" + uuid.NewV4().String(),
		ExchangeName:  exchangeName,
		QueueName:     queueName,
		RoutingKeys:   routingKeys,
		prefetchCount: prefetchCount,
		durable:       durable,
		noWait:        noWait,
	}

	// 声明交换机
	mq.exchangeDeclare()

	// 声明队列
	mq.queueDeclare()

	// 用路由绑定交换机
	for _, r := range routingKeys {
		if err := mq.channel.QueueBind(
			mq.QueueName,    // 队列名
			r,               // topic的路由规则
			mq.ExchangeName, // 交换机
			mq.noWait,       // 阻塞
			nil,             // 其他参数
		); err != nil {
			mq.failOnError(err, "queue bind exchange failed.")
			panic(err)
		}
	}

	// 配置消费速率
	mq.channel.Qos(prefetchCount, 0, false)
	return mq

}

/* 声明交换机 */
func (mq *topicConsumeMQ) exchangeDeclare() {
	if err := mq.channel.ExchangeDeclare(
		mq.ExchangeName, // 交换机名称
		"topic",         // 交换机类型，该模式必须topic
		mq.durable,      // 交换机持久标记
		false,           // 自动删除
		false,           // 仅rabbitMQ内部使用
		mq.noWait,       // 阻塞
		nil,             // 额外参数
	); err != nil {
		mq.failOnError(err, "declare exchange failed.")
		panic(err)
	}
}

func (mq *topicConsumeMQ) queueDeclare() {
	var err error
	mq.queue, err = mq.channel.QueueDeclare(
		mq.QueueName,
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
}

/* 创建消费者消息管道 */
func (mq *topicConsumeMQ) TopicChan() (<-chan amqp.Delivery, error) {
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
