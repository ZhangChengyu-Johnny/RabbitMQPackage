package PublishMQ

import (
	"context"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	uuid "github.com/satori/go.uuid"
)

type routingPublish struct {
	*basicPublish
	durable bool
	noWait  bool
}

// 路由模式中每个生产者绑定一个direct类型的交换机，向交换机发布消息时需指定RoutingKey
// 交换机会根据RoutingKey将消息发布到绑定该RoutingKey的队列

func NewRoutingPublishMQ(exchangeName string, durable, noWait bool) *routingPublish {
	mq := &routingPublish{
		basicPublish: newBasicPublishMQ(exchangeName, "routing-publish"),
	}
	mq.exchangeDeclare()

	return mq
}

func (mq *routingPublish) exchangeDeclare() {
	if err := mq.channel.ExchangeDeclare(
		mq.ExchangeName, // 交换机名称
		"direct",        // 交换机类型
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

func (mq *routingPublish) DirectPublish(message, routingKey string) error {
	if err := mq.channel.PublishWithContext(
		context.Background(),
		mq.ExchangeName, // 发到绑定的交换器
		routingKey,      // RoutingKey为空，交换机将发给所有绑定的队列
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
