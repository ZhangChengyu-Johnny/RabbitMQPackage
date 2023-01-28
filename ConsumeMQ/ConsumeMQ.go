package ConsumeMQ

import (
	"RabbitMQPackage/Config"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type basicConsume struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	dns     string
	Role    string
}

func newBasicConsumeMQ(role string) *basicConsume {
	var err error
	mq := &basicConsume{
		dns:  Config.MQDNS,
		Role: role,
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

	return mq

}

func (mq *basicConsume) failOnError(e error, errorMsg string) {
	log.Println("error:", e)
	log.Println("error message:", errorMsg)
}

func (mq *basicConsume) Destory() {
	mq.channel.Close()
	mq.conn.Close()
}

/* 确认消息，MQ收到回复后才删除消息 */
func (mq *basicConsume) Ack(deliveryTag uint64, multiple bool) {
	/*
		deliveryTag: 信道的消息计数器
		multiple: 是否批量确认

		单条确认模式: 单次确认处理完成的消息
		批量确认模式: 批量确认该消息及之前读出的所有消息
	*/
	mq.channel.Ack(deliveryTag, multiple)
}

/* 拒绝消息，包含批量功能 */
func (mq *basicConsume) Nack(deliveryTag uint64, multiple, requeue bool) {
	/*
		deliveryTag: 信道的消息计数器
		multiple: 是否批量拒绝
		requeue: 被拒绝的消息是否重回队列

		单条拒绝模式: 单条拒绝消息
		批量应答模式: 批量拒绝该消息及之前读出的所有消息
		重新入队模式: 被拒绝的消息重新入队，再次分配给消费者
		死信队列模式: 被拒绝的消息加入死信队列，不再重新分配
	*/
	mq.channel.Nack(deliveryTag, multiple, requeue)
}

/* 拒绝消息，仅支持单条拒绝模式 */
func (mq *basicConsume) Reject(deliveryTag uint64, requeue bool) {
	/*
		deliveryTag: 信道的消息计数器
		requeue: 被拒绝的消息是否重回队列

		重新入队模式: 被拒绝的消息重新入队，再次分配给消费者
		死信队列模式: 被拒绝的消息加入死信队列，不再重新分配
	*/
	mq.channel.Reject(deliveryTag, requeue)
}
