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

type ConfirmPublishMQ struct {
	*basicPublish
	Queues        map[string]struct{}    // 工作模式生产者对应多个队列
	notifyConfirm chan amqp.Confirmation // 接收发布结果的管道
	MessageMap    sync.Map               // 临时存储发布消息的管道
}

type RecordMethod func(string)

func NewConfirmPublishMQ() *ConfirmPublishMQ {
	mq := &ConfirmPublishMQ{
		basicPublish: newBasicPublishMQ("", "confirm-publish"),
		Queues:       make(map[string]struct{}),
	}
	// 把notifyConfirm管道加入监听队列
	mq.notifyConfirm = mq.channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	// 监听发布结果的goroutine
	go mq.GetPublishResult(mq.notifyConfirm)

	if err := mq.channel.Confirm(false); err != nil {
		mq.failOnError(err, "confirm failed")
		return nil
	}

	return mq

}

/* 监听发布结果 */
func (mq *ConfirmPublishMQ) GetPublishResult(c <-chan amqp.Confirmation) {
	for confirmRet := range c {
		if confirmRet.Ack {
			log.Println("publish message successful. deliveryTag: ", confirmRet.DeliveryTag)
			mq.MessageMap.Delete(confirmRet.DeliveryTag)
		} else {
			msg, _ := mq.MessageMap.Load(confirmRet.DeliveryTag)
			log.Println("publish message failed. deliveryTag: ", msg)
			// TODO: 重发消息或持久化
			// mq.MessageMap.LoadAndDelete(confirmRet.DeliveryTag)
		}
	}
}

/* 使用default交换机发布 */
func (mq *ConfirmPublishMQ) QueueDeclare(queueName string, durable, noWait bool, args amqp.Table) error {
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

/* 获取下一个信道ID */
func (mq *ConfirmPublishMQ) GetNexPublishSeqNo() uint64 {
	return mq.channel.GetNextPublishSeqNo()
}

func (mq *ConfirmPublishMQ) ConfirmPublishMessage(message, queueName string) error {
	if _, ok := mq.Queues[queueName]; !ok {
		err := errors.New("queue not exists")
		mq.failOnError(err, "queue not exists")
		return err
	}

	data := &amqp.Publishing{
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
	}
	// 存储消息，在标记发布成功后删除
	mq.MessageMap.Store(mq.GetNexPublishSeqNo(), data)
	if err := mq.channel.PublishWithContext(
		context.Background(),
		"",        // 交换器
		queueName, // 路由
		false,     // 开启后把无法找到符合路由的消息返回给生产者
		false,     // 开启后如果交换机发送的队列上都没有消费者，那么把消息返回给生产者
		*data,
	); err != nil {
		mq.failOnError(err, "publish message failed.")
		return err
	}
	return nil
}
