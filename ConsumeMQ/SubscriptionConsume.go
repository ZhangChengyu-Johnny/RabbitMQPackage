package ConsumeMQ

import (
	amqp "github.com/rabbitmq/amqp091-go"
	uuid "github.com/satori/go.uuid"
)

// 发布订阅模式中每个消费者声明一个fanout类型的交换机和一个(随机名称)的队列，使用空的RoutingKey绑定
// 交换机会将消息转发到所有绑定的队列

type subscriptionConsumeMQ struct {
	*basicConsume
}

func NewSubscriptionConsumeMQ(exchangeName string, prefetchCount int, durable, noWait, deadQueue bool) *subscriptionConsumeMQ {
	mq := &subscriptionConsumeMQ{
		basicConsume: newBasicConsumeMQ("fanout-consume", exchangeName, uuid.NewV4().String(), durable, noWait, prefetchCount, deadQueue),
	}

	// 交换机、队列、路由 绑定
	if err := mq.channel.QueueBind(
		mq.QueueName,    // 随机生成的队列名
		"",              // RoutingKey，该模式用空
		mq.ExchangeName, // 交换机名
		noWait,          // 阻塞
		nil,
	); err != nil {
		mq.failOnError(err, "queue bind exchange failed.")
		panic(err)
	}

	return mq
}

/* 创建消费者消息管道 */
func (mq *subscriptionConsumeMQ) FanoutChan() (<-chan amqp.Delivery, error) {
	return mq.messageChan()
}
