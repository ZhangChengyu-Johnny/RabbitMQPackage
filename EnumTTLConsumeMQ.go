package RabbitMQPackage

import (
	"strconv"

	amqp "github.com/rabbitmq/amqp091-go"
)

// 枚举超时消费者公有1个正常队列 + 1个direct类型的交换机
// 另外需要将n个延迟队列绑定到该交换机，n个延迟队列上层再绑定1个交换机按需对数据延迟
// 延迟队列默认都持久化
type ttlEnumConsume struct {
	*basicConsume

	DelayQueuesExchangeName     string                // 延迟队列入口的交换机
	DelayQueuesExpirationConfig map[string]string     // 延迟队列配置，队列名:延迟时间(毫秒)
	delayQueues                 map[string]amqp.Queue // 延迟队列，队列名:队列实例
}

func NewTTLConsumeMQNewConsumeMQ(exchangeName, queueName string, prefetchCount int, deadQueue bool,
	delayexchangeName string, queuesExpirationConf map[string]string) *ttlEnumConsume {
	durable := true
	noWait := false
	routingKey := queueName + "-delay-rk"
	mq := newBasicConsumeMQ("direct", exchangeName, queueName, []string{routingKey}, durable, noWait, prefetchCount, deadQueue)
	ttlMQ := &ttlEnumConsume{
		basicConsume:                mq,
		DelayQueuesExchangeName:     delayexchangeName,
		DelayQueuesExpirationConfig: queuesExpirationConf,
		delayQueues:                 make(map[string]amqp.Queue),
	}

	// 声明延迟队列入口的交换机
	ttlMQ.exchangeDeclare(ttlMQ.DelayQueuesExchangeName, "direct", ttlMQ.durable, ttlMQ.noWait)
	// 循环创建延迟队列，并绑定TTL队列交换机
	for queueName, expiration := range ttlMQ.DelayQueuesExpirationConfig {
		var ttlExchangeConfig map[string]any = make(map[string]any)
		ttlExchangeConfig["x-dead-letter-exchange"] = ttlMQ.ExchangeName
		ttlExchangeConfig["x-dead-letter-routing-key"] = routingKey
		expirationInt, _ := strconv.Atoi(expiration)
		ttlExchangeConfig["x-message-ttl"] = expirationInt
		ttlMQ.delayQueues[queueName] = ttlMQ.queueDeclare(queueName, durable, noWait, ttlExchangeConfig)

		// 绑定
		ttlMQ.channel.QueueBind(
			queueName,
			queueName+"-rk",
			ttlMQ.DelayQueuesExchangeName,
			false,
			nil,
		)
	}

	return ttlMQ

}
