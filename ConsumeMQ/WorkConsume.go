package ConsumeMQ

import (
	"errors"

	amqp "github.com/rabbitmq/amqp091-go"
	uuid "github.com/satori/go.uuid"
)

type WorkConsumeMQ struct {
	*basicConsume
	Queues        map[string]struct{}
	prefetchCount int // 直到返回ack，才能继续给prefetch_count条message，0表示RR
	prefetchSize  int // 设置消费者未确认消息内存大小的上限，单位Byte，0表示没有下限
}

func NewWorkConsumeMQ(prefetchCount, prefetchSize int) *WorkConsumeMQ {
	mq := newBasicConsumeMQ("work-consume")
	// global默认false，只在本consume生效
	if err := mq.channel.Qos(prefetchCount, prefetchSize, false); err != nil {
		mq.failOnError(err, "Qos failed.")
		panic(err)
	}
	return &WorkConsumeMQ{
		basicConsume:  mq,
		Queues:        make(map[string]struct{}),
		prefetchCount: prefetchCount,
		prefetchSize:  prefetchSize,
	}
}

/* 为消费者创建队列 */
func (mq *WorkConsumeMQ) QueueDeclare(queueName string, durable, noWait bool, args amqp.Table) error {
	if _, ok := mq.Queues[queueName]; ok {
		return nil
	}
	if _, err := mq.channel.QueueDeclare(
		queueName, // 队列名称
		durable,   // 队列持久化标记
		false,     // 自动删除
		false,     // 队列独占标记
		noWait,    // 阻塞
		args,      // 额外参数
	); err != nil {
		mq.failOnError(err, "declare queue failed.")
		return err
	}

	mq.Queues[queueName] = struct{}{}
	return nil
}

/* 创建消费者消息管道 */
func (mq *WorkConsumeMQ) DefaultChan(queueName string, noWait bool, args amqp.Table) (msgChan <-chan amqp.Delivery, err error) {
	if _, ok := mq.Queues[queueName]; !ok {
		err = errors.New("queue not exists")
		mq.failOnError(err, "queue not exists")
		return
	}

	// 接收消息
	msgChan, err = mq.channel.Consume(
		queueName,                     // 队列名称
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
