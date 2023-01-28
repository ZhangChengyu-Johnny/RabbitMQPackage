package PublishMQ

import (
	"context"
	"errors"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	uuid "github.com/satori/go.uuid"
)

type WorkPublishMQ struct {
	*basicPublish
	Queues map[string]struct{} // 工作模式生产者对应多个队列
}

func NewWorkPublishMQ() *WorkPublishMQ {
	basicMQ := newBasicPublishMQ("", "work-publisher")
	return &WorkPublishMQ{
		basicPublish: basicMQ,
		Queues:       make(map[string]struct{}),
	}
}

/* 使用default交换机发布 */
func (mq *WorkPublishMQ) QueueDeclare(queueName string, durable, noWait bool, args amqp.Table) error {
	if _, ok := mq.Queues[queueName]; ok {
		return nil
	}
	if _, err := mq.channel.QueueDeclare(
		queueName, // 队列名称
		durable,   // 队列持久化标记
		false,     // 自动删除
		false,     // 队列独占标记
		noWait,    // 阻塞
		args,      // 额外参数
	); err != nil {
		mq.failOnError(err, "declare queue failed.")
		return err
	}

	mq.Queues[queueName] = struct{}{}
	return nil
}

func (mq *WorkPublishMQ) WorkPublishMessage(message, queueName string) error {
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
