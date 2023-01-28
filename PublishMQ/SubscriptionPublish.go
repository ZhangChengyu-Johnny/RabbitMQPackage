package PublishMQ

import amqp "github.com/rabbitmq/amqp091-go"

type SubscriptionPublish struct {
	*basicPublish
}

func NewSubscriptionPublishMQ(exchangeName string) *SubscriptionPublish {
	mq := &SubscriptionPublish{
		basicPublish: newBasicPublishMQ(exchangeName, "subscription-publish"),
	}
	mq.exchangeDeclare()
	return mq
}

func (mq *SubscriptionPublish) exchangeDeclare(durable, noWait bool, args amqp.Table) {
	mq.channel.ExchangeDeclare(
		mq.ExchangeName, // 交换机名称
		"fanout",        // 交换机类型
		durable,         // 交换机持久化标记
		false,           // 自动删除
		false,           // 仅rabbitMQ内部使用
		noWait,          // 阻塞
		args,            // 额外参数
	)
}
