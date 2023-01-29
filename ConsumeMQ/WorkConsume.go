package ConsumeMQ

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

// 工作模式中每个消费者绑定一个default类型的交换机和一个队列
// 交换器将会转发消息到指定队列

type workConsumeMQ struct {
	*basicConsume
}

func NewWorkConsumeMQ(queueName string, prefetchCount int, durable, noWait bool) *workConsumeMQ {
	return &workConsumeMQ{
		basicConsume: newBasicConsumeMQ("default-consume", "", queueName, durable, noWait, prefetchCount, false),
	}
}

/* 创建消费者消息管道 */
func (mq *workConsumeMQ) DefaultChan() (<-chan amqp.Delivery, error) {
	return mq.messageChan()
}
