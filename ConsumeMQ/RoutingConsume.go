package ConsumeMQ

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

// 路由模式中的每个消费者声明一个队列和一个direct类型的交换机，使用RoutingKey将两者绑定
// 交换机会根据RoutingKey把消息转发到指定队列

type routingConsumeMQ struct {
	*basicConsume
	RoutingKey string // 路由名

}

func NewRoutingConsumeMQ(exchangeName, queueName, routingKey string, prefetchCount int, durable, noWait, deadQueue bool) *routingConsumeMQ {
	mq := &routingConsumeMQ{
		basicConsume: newBasicConsumeMQ("direct-consume", exchangeName, queueName, durable, noWait, prefetchCount, deadQueue),
		RoutingKey:   routingKey,
	}

	// 交换机、队列、路由 绑定
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

	return mq
}

/* 创建消费者消息管道 */
func (mq *routingConsumeMQ) RoutingChan() (<-chan amqp.Delivery, error) {
	return mq.messageChan()
}
