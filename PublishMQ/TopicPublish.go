package PublishMQ

import (
	"context"
	"errors"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	uuid "github.com/satori/go.uuid"
)

// 主体模式中每个生产者绑定一个topics类型的交换机，向交换机发送路由时需指定RoutingKey匹配规则
// 交换机会把消息发送给所有命中匹配规则的队列

type routingKey string

type topicPublishMQ struct {
	*basicPublish
	routingKeys map[routingKey]struct{}
	durable     bool
	noWait      bool
}

func NewTopicPublishMQ(exchangeName string, routingKeys []string, durable, noWait bool) *topicPublishMQ {
	mq := &topicPublishMQ{
		basicPublish: newBasicPublishMQ(exchangeName, "topic-publish"),
		routingKeys:  make(map[routingKey]struct{}),
		durable:      durable,
		noWait:       noWait,
	}

	for _, r := range routingKeys {
		mq.routingKeys[routingKey(r)] = struct{}{}
	}
	if len(mq.routingKeys) == 0 {
		err := errors.New("RoutingKey is required")
		mq.failOnError(err, "RoutingKey is required")
		panic(err)
	}

	// 声明交换机
	mq.exchangeDeclare()
	return mq
}

func (mq *topicPublishMQ) exchangeDeclare() {
	if err := mq.channel.ExchangeDeclare(
		mq.ExchangeName, // 交换机名称
		"topic",         // 交换机类型
		mq.durable,      // 交换机持久化标记
		false,           // 自动删除
		false,           // 仅rabbitMQ内部使用
		mq.noWait,       // 阻塞
		nil,             // 额外参数
	); err != nil {
		mq.failOnError(err, "declare exchange failed.")
		panic(err)
	}
}

func (mq *topicPublishMQ) GetRoutingKey(rk string) routingKey {
	if _, ok := mq.routingKeys[routingKey(rk)]; ok {
		return routingKey(rk)
	} else {
		return ""
	}
}

func (mq *topicPublishMQ) TopicPublish(message string, rk routingKey) error {
	if err := mq.channel.PublishWithContext(
		context.Background(),
		mq.ExchangeName, // 发到绑定的交换器
		string(rk),      // RoutingKey匹配规则
		false,           // 开启后把无法找到符合路由的消息返回给生产者
		false,           // 开启后如果交换机发送的队列上都没有消费者，那么把消息返回给生产者
		amqp.Publishing{
			ContentType:  "text/plain",          // 消息内容类型
			DeliveryMode: 1,                     // 持久设置，1:临时消息；2:持久化
			Priority:     0,                     // 消息优先级0~9
			ReplyTo:      "",                    // address to reply(ex: RPC)
			Expiration:   "",                    // 消息有效期
			MessageId:    uuid.NewV4().String(), // message identity
			Timestamp:    time.Now(),            // 发送消息的时间
			Type:         "",                    // 消息类型
			UserId:       "",                    // user identity(ex: admin)
			AppId:        "",                    // app identity
			Body:         []byte(message),       // 消息内容
		},
	); err != nil {
		mq.failOnError(err, "publish message failed.")
		return err
	}
	return nil
}
