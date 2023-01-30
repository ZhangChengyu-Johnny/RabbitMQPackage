package RabbitMQPackage

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	uuid "github.com/satori/go.uuid"
)

/* 1个生产者 = 1个交换机 n个路由 */
type basicPublish struct {
	conn          *amqp.Connection
	channel       *amqp.Channel
	dns           string
	publisherName string
	ExchangeName  string
	ExchangeType  string                 // 交换机类型
	RoutingKeys   map[string]struct{}    // 路由
	durable       bool                   // 持久化配置
	noWait        bool                   // 阻塞配置
	message       *amqp.Publishing       // 消息主体
	MessageMap    sync.Map               // 缓存消息，信道ID:消息实例
	confirm       bool                   // 是否发布确认
	notifyConfirm chan amqp.Confirmation // 接收发布结果的管道
	AckCounter    uint32
}

func newBasicPublishMQ(exchangeType, exchangeName string, routingKeys []string, durable, noWait, confirm bool) *basicPublish {
	var err error
	mq := &basicPublish{
		dns:           MQDNS,
		publisherName: exchangeName + "-publish-" + uuid.NewV4().String(),
		ExchangeName:  exchangeName,
		ExchangeType:  exchangeType,
		RoutingKeys:   make(map[string]struct{}),
		durable:       durable,
		noWait:        noWait,
		confirm:       confirm,
		message:       &amqp.Publishing{},
	}

	// 建立TCP连接
	mq.conn, err = amqp.Dial(mq.dns)
	if err != nil {
		mq.failOnError(err, "rabbitMQ connection failed.")
		panic(err)
	}

	// 建立信道连接
	mq.channel, err = mq.conn.Channel()
	if err != nil {
		mq.failOnError(err, "make channel failed.")
		panic(err)
	}

	// 声明交换机
	mq.exchangeDeclare(mq.ExchangeName, mq.ExchangeType, durable, noWait)

	// 注册路由
	mq.RegisterRoutingKey(routingKeys)

	// 初始化消息配置，是否持久化
	if durable {
		mq.message.DeliveryMode = uint8(2)
	} else {
		mq.message.DeliveryMode = uint8(1)
	}

	// 开启发布确认模式
	if confirm {
		mq.notifyConfirm = mq.channel.NotifyPublish(make(chan amqp.Confirmation, 100))
		go mq.publishConfirm(mq.notifyConfirm)

		if err := mq.channel.Confirm(false); err != nil {
			mq.failOnError(err, "publisher channel could not be put into confirm mode.")
		}
	}

	return mq

}

/* 工厂模式实例化 */
func NewPublishMQ(mode Mode, exchangeName string, routingKeys []string, durable, noWait, confirm bool) *basicPublish {
	switch mode {
	case SimpleMode, WorkMode, ConfirmMode, RoutingMode:
		return newBasicPublishMQ("direct", exchangeName, routingKeys, durable, noWait, confirm)
	case SubscriptionMode:
		return newBasicPublishMQ("fanout", exchangeName, routingKeys, durable, noWait, confirm)
	case TopicMode:
		return newBasicPublishMQ("topic", exchangeName, routingKeys, durable, noWait, confirm)
	default:
		panic(errors.New("mode error"))
	}
}

/* 获取下一个信道ID */
func (publisher *basicPublish) GetNexPublishSeqNo() uint64 {
	return publisher.channel.GetNextPublishSeqNo()
}

/* 生产者声明交换机 */
func (publisher *basicPublish) exchangeDeclare(exchangeName, exchangeType string, durable, noWait bool) {
	if err := publisher.channel.ExchangeDeclare(
		exchangeName, // 交换机名称
		exchangeType, // 交换机类型 [default direct fanout topic]
		durable,      // 交换机持久标记
		false,        // 自动删除
		false,        // 仅rabbitMQ内部使用
		noWait,       // 阻塞
		nil,          // 额外参数
	); err != nil {
		publisher.failOnError(err, "declare exchange failed.")
		panic(err)
	}
}

/* 注册路由 */
func (publisher *basicPublish) RegisterRoutingKey(routingKeys []string) {
	for k := range publisher.RoutingKeys {
		delete(publisher.RoutingKeys, k)
	}
	for _, route := range routingKeys {
		publisher.RoutingKeys[route] = struct{}{}
	}
}

/* amqp包中发布消息采用结构体的值传递，所以封装上可以复用一个消息实体，不需要来回开辟内存 */
func (publish *basicPublish) CreateMessage(message, contentType string, priorityLevel, expriation int) *amqp.Publishing {
	/*
		amqp.Publishing{
			ContentType:  "text/plain",          // 消息内容类型
			DeliveryMode: 1,                     // 持久设置，1:临时消息；2:持久化
			Priority:     0,                     // 消息优先级0~9
			ReplyTo:      "",                    // address to reply(ex: RPC)
			Expiration:   "",                    // 消息有效期(单位毫秒)
			MessageId:    uuid.NewV4().String(), // message identity
			Timestamp:    time.Now(),            // 发送消息的时间
			Type:         "",                    // 消息类型
			UserId:       "",                    // user identity(ex: admin)
			AppId:        "",                    // app identity
			Body:         []byte(message),       // 消息内容
		}
	*/
	publish.message.ContentType = contentType
	publish.message.Priority = uint8(priorityLevel)
	publish.message.Expiration = strconv.Itoa(expriation)
	publish.message.MessageId = uuid.NewV4().String()
	publish.message.Timestamp = time.Now()
	publish.message.Body = []byte(message)

	return publish.message
}

/* 发布消息 */
func (publisher *basicPublish) Publish(message *amqp.Publishing, routingKey string, ctx context.Context) error {
	if _, ok := publisher.RoutingKeys[routingKey]; !ok {
		err := errors.New("must register routingKey first")
		publisher.failOnError(err, "routingKey not exists.")
		return err
	}

	newMessage := *message
	if publisher.confirm {
		// 如果开启发布确认，消息在确认前缓存到内存
		publisher.MessageMap.Store(publisher.GetNexPublishSeqNo(), newMessage)

	}
	err := publisher.channel.PublishWithContext(
		ctx,                    // 可控制发送超时
		publisher.ExchangeName, // 交换器
		routingKey,             // 路由
		false,                  // 将无法匹配路由的消息返回给生产者
		false,                  // 将没有绑定消费者队列的消息返回给生产者
		newMessage,             // 消息主体
	)

	if err != nil {
		publisher.failOnError(err, "publish message failed.")
		return err
	}

	return nil
}

/* 发布确认goroutine */
func (publisher *basicPublish) publishConfirm(confirmChan <-chan amqp.Confirmation) {
	for ret := range confirmChan {
		if ret.Ack {
			msg, _ := publisher.MessageMap.LoadAndDelete(ret.DeliveryTag)
			publisher.AckCounter++
			fmt.Println("发送成功: ", string(msg.(amqp.Publishing).Body))
		} else {
			msg, _ := publisher.MessageMap.Load(ret.DeliveryTag)
			fmt.Println("发送失败: ", string(msg.(amqp.Publishing).Body))
		}
	}
}

/* 异常日志 */
func (publisher *basicPublish) failOnError(e error, errorMsg string) {
	log.Println("error:", e)
	log.Println("error message:", errorMsg)
}

/* 关闭方法 */
func (publisher *basicPublish) Destory() {
	publisher.channel.Close()
	publisher.conn.Close()
}
