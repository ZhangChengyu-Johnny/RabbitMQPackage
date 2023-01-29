package ConsumeMQ

import (
	amqp "github.com/rabbitmq/amqp091-go"
	uuid "github.com/satori/go.uuid"
)

// 工作模式中每个消费者绑定一个default类型的交换机和一个队列
// 交换器将会转发消息到指定队列

type workConsumeMQ struct {
	*basicConsume
	consumeName   string
	QueueName     string
	queue         amqp.Queue
	prefetchCount int // 直到返回ack，才能继续给prefetch_count条message，0表示RR
	durable       bool
	noWait        bool
}

func NewWorkConsumeMQ(queueName string, prefetchCount int, durable, noWait bool) *workConsumeMQ {
	mq := &workConsumeMQ{
		basicConsume:  newBasicConsumeMQ("work-consume"),
		consumeName:   "work-consume-" + uuid.NewV4().String(),
		prefetchCount: prefetchCount,
		durable:       durable,
		noWait:        noWait,
	}

	// 声明队列
	mq.queueDeclare(queueName, durable, noWait)

	// 配置消费速率
	if err := mq.channel.Qos(prefetchCount, 0, false); err != nil {
		mq.failOnError(err, "Qos failed.")
		panic(err)
	}
	return mq
}

/* 为消费者创建队列 */
func (mq *workConsumeMQ) queueDeclare(queueName string, durable, noWait bool) {
	var err error
	mq.queue, err = mq.channel.QueueDeclare(
		queueName, // 队列名称
		durable,   // 队列持久化标记
		false,     // 自动删除
		false,     // 队列独占标记
		noWait,    // 阻塞
		nil,       // 额外参数
	)

	if err != nil {
		mq.failOnError(err, "declare queue failed.")
		panic(err)
	}
	mq.QueueName = mq.queue.Name
}

/* 创建消费者消息管道 */
func (mq *workConsumeMQ) DefaultChan() (<-chan amqp.Delivery, error) {
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
