package ConsumeMQ

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

// 主体模式中每个消费者声明一个topics类型的交换机和一个队列，该队列可以绑定多个RoutingKey
// 交换机会将消息转发到所有命中匹配规则的队列

type topicConsumeMQ struct {
	*basicConsume
	RoutingKeys []string
}

func NewTopicConsumeMQ(exchangeName, queueName string, routingKeys []string,
	prefetchCount int, durable, noWait, deadQueue bool) *topicConsumeMQ {
	mq := &topicConsumeMQ{
		basicConsume: newBasicConsumeMQ("topic-consumer", exchangeName, queueName, durable, noWait, prefetchCount, deadQueue),
		RoutingKeys:  routingKeys,
	}

	// 交换机、队列、路由 绑定
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

	return mq

}

/* 创建消费者消息管道 */
func (mq *topicConsumeMQ) TopicChan() (<-chan amqp.Delivery, error) {
	return mq.messageChan()
}
