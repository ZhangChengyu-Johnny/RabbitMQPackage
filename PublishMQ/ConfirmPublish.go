package PublishMQ

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	uuid "github.com/satori/go.uuid"
)

// 发布确认模式中每个生产者绑定一个default类型的交换机，可以储存多个队列信息，根据队列名将消息发布到指定队列

type confirmPublishMQ struct {
	*basicPublish
	Queues        map[string]struct{}    // 工作模式生产者对应多个队列
	notifyConfirm chan amqp.Confirmation // 接收发布结果的管道
	MessageMap    sync.Map               // 临时存储发布消息的管道
	TestCounter   int
}

func NewConfirmPublishMQ() *confirmPublishMQ {
	mq := &confirmPublishMQ{
		basicPublish: newBasicPublishMQ("", "default-publish"),
		Queues:       make(map[string]struct{}),
	}
	// 把notifyConfirm管道加入监听队列
	mq.notifyConfirm = mq.channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	// 监听发布结果的goroutine
	go mq.publishResult(mq.notifyConfirm)

	// 开启发布确认模式
	if err := mq.channel.Confirm(false); err != nil {
		mq.failOnError(err, "confirm failed")
		return nil
	}

	return mq

}

/* 监听发布结果 */
func (mq *confirmPublishMQ) publishResult(c <-chan amqp.Confirmation) {
	for confirmRet := range c {
		if confirmRet.Ack {
			mq.TestCounter++
			mq.MessageMap.Delete(confirmRet.DeliveryTag)
		} else {
			msg, _ := mq.MessageMap.LoadAndDelete(confirmRet.DeliveryTag)
			log.Println("publish message failed. deliveryTag: ", msg.(map[string]string)["message"])
			// TODO: 重发消息或持久化
			mq.DefaultPublish(msg.(map[string]string)["queueName"], msg.(map[string]string)["message"])
		}
	}
}

/* 提供给用户注册多个队列 */
func (mq *confirmPublishMQ) QueueDeclare(queueName string, durable, noWait bool) error {
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

/* 获取下一个信道ID */
func (mq *confirmPublishMQ) GetNexPublishSeqNo() uint64 {
	return mq.channel.GetNextPublishSeqNo()
}

func (mq *confirmPublishMQ) DefaultPublish(message, queueName string) error {
	if _, ok := mq.Queues[queueName]; !ok {
		err := errors.New("queue not exists")
		mq.failOnError(err, "queue not exists")
		return err
	}

	// 存储消息，在标记发布成功后删除
	mq.MessageMap.Store(mq.GetNexPublishSeqNo(), map[string]string{"message": message, "queueName": queueName})
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
