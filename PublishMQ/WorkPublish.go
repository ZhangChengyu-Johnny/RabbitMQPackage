package PublishMQ

import (
	"context"
	"errors"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	uuid "github.com/satori/go.uuid"
)

// 工作模式中每个生产者绑定一个default类型的交换机，可以储存多个队列信息，根据队列名将消息发布到指定队列
// 工作模式的生产者需要配置队列，所以不支持死信模式

type workPublishMQ struct {
	*basicPublish
	Queues map[string]struct{} // 工作模式生产者对应多个队列
}

func NewWorkPublishMQ() *workPublishMQ {
	return &workPublishMQ{
		basicPublish: newBasicPublishMQ("", "default-publisher"),
		Queues:       make(map[string]struct{}),
	}
}

/* 提供给用户注册多个队列 */
func (mq *workPublishMQ) QueueDeclare(queueName string, durable, noWait bool) error {
	if _, ok := mq.Queues[queueName]; ok {
		return nil
	}
	if _, err := mq.channel.QueueDeclare(
		queueName, // 队列名称
		durable,   // 队列持久化标记
		false,     // 自动删除
		false,     // 队列独占标记
		noWait,    // 阻塞
		nil,       // 额外参数
	); err != nil {
		mq.failOnError(err, "declare queue failed.")
		return err
	}
	mq.Queues[queueName] = struct{}{}
	return nil
}

func (mq *workPublishMQ) DefaultPublish(message, queueName string) error {
	if _, ok := mq.Queues[queueName]; !ok {
		err := errors.New("queue not exists")
		mq.failOnError(err, "queue not exists")
		return err
	}
	if err := mq.channel.PublishWithContext(
		context.Background(),
		"",        // 交换器
		queueName, // 路由
		false,     // 开启后把无法找到符合路由的消息返回给生产者
		false,     // 开启后如果交换机发送的队列上都没有消费者，那么把消息返回给生产者
		amqp.Publishing{
			ContentType:  "text/plain",          // 消息内容类型
			DeliveryMode: 1,                     // 持久设置，1:临时消息；2:持久化
			Priority:     0,                     // 消息优先级0~9
			ReplyTo:      "",                    // address to reply(ex: RPC)
			Expiration:   "",                    // 消息有效期，毫秒
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
