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
	Queues  map[string]struct{}
	Role    string
}

func newBasicConsumeMQ(role string) *basicConsume {
	var err error
	mq := &basicConsume{
		dns:    Config.MQDNS,
		Queues: make(map[string]struct{}),
		Role:   role,
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
