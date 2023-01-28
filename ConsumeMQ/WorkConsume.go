package ConsumeMQ

import (
	"errors"

	amqp "github.com/rabbitmq/amqp091-go"
)

type WorkConsumeMQ struct {
	*basicConsume
	prefetchCount int // 直到返回ack，才能继续给prefetch_count条message，0表示RR
	prefetchSize  int // 设置消费者未确认消息内存大小的上限，单位Byte，0表示没有下限
}

func NewWorkConsumeMQ(prefetchCount, prefetchSize int) *WorkConsumeMQ {
	mq := newBasicConsumeMQ("work-consume")
	// global默认false，只在本consume生效
	mq.channel.Qos(prefetchCount, prefetchSize, false)
	return &WorkConsumeMQ{
		basicConsume:  mq,
		prefetchCount: prefetchCount,
		prefetchSize:  prefetchSize,
	}
}

/* 为消费者创建队列 */
func (mq *WorkConsumeMQ) QueueDeclare(queueName string, durable, autuDelete, exclusive, noWait bool, args amqp.Table) error {
	if _, ok := mq.Queues[queueName]; ok {
		return nil
	}
	if _, err := mq.channel.QueueDeclare(
		queueName,  // 队列名称
		durable,    // 队列持久化标记
		autuDelete, // 自动删除
		exclusive,  // 队列独占标记
		noWait,     // 阻塞
		args,       // 额外参数
	); err != nil {
		mq.failOnError(err, "declare queue failed.")
		return err
	}

	mq.Queues[queueName] = struct{}{}
	return nil
}

/* 创建消费者消息管道 */
func (mq *WorkConsumeMQ) WorkMessageChan(consumeName, queueName string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (msgChan <-chan amqp.Delivery, err error) {
	if _, ok := mq.Queues[queueName]; !ok {
		err = errors.New("queue not exists")
		mq.failOnError(err, "queue not exists")
		return
	}

	// 接收消息
	msgChan, err = mq.channel.Consume(
		queueName,   // 队列名称
		consumeName, // 消费者名称
		false,       // 自动应答
		false,       // 独占
		false,       // 开启后同一个connection不能传递消息
		false,       // 消费队列是否阻塞(msgChan)
		nil,         // 其他参数
	)
	if err != nil {
		mq.failOnError(err, "make message channel failed.")
		return
	}
	return
}

/* 确认消息，MQ收到回复后才删除消息 */
func (mq *WorkConsumeMQ) Ack(deliveryTag uint64, multiple bool) {
	/*
		deliveryTag: 信道的消息计数器
		multiple: 是否批量确认

		单条确认模式: 单次确认处理完成的消息
		批量确认模式: 批量确认该消息及之前读出的所有消息
	*/
	mq.channel.Ack(deliveryTag, multiple)
}

/* 拒绝消息，包含批量功能 */
func (mq *WorkConsumeMQ) Nack(deliveryTag uint64, multiple, requeue bool) {
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
func (mq *WorkConsumeMQ) Reject(deliveryTag uint64, requeue bool) {
	/*
		deliveryTag: 信道的消息计数器
		requeue: 被拒绝的消息是否重回队列

		重新入队模式: 被拒绝的消息重新入队，再次分配给消费者
		死信队列模式: 被拒绝的消息加入死信队列，不再重新分配
	*/
	mq.channel.Reject(deliveryTag, requeue)
}
