package PublishMQ

import (
	"RabbitMQPackage/Config"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type basicPublish struct {
	conn         *amqp.Connection
	channel      *amqp.Channel
	dns          string
	ExchangeName string // 每个生产者对应一个交换机
	Role         string
}

func newBasicPublishMQ(exchangeName, role string) *basicPublish {
	var err error
	mq := &basicPublish{
		dns:          Config.MQDNS,
		ExchangeName: exchangeName,
		Role:         role,
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

func (mq *basicPublish) failOnError(e error, errorMsg string) {
	log.Println("error:", e)
	log.Println("error message:", errorMsg)
}

func (mq *basicPublish) Destory() {
	mq.channel.Close()
	mq.conn.Close()
}
