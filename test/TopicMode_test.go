package test

import (
	"RabbitMQPackage/ConsumeMQ"
	"RabbitMQPackage/PublishMQ"
	"log"
	"testing"
	"time"
)

// routingKey 1: *.orange.*
// routingKey 2: *.*.rabbit
// routingKey 3: lazy.#

func TestTopicP(t *testing.T) {
	excahngeName := "topic-mode-exchange-test"
	durable := false
	noWait := false
	expiration := "5000"

	routingKeys := []string{
		"quick.orange.rabbit",      // 命中Q1 Q2
		"lazy.orange.elephant",     // 命中Q1 Q2
		"quick.orange.fox",         // 命中Q1
		"lazy.brown.fox",           // 命中Q2
		"lazy.pink.rabbit",         // 命中Q2
		"quick.brown.fox",          // 都未命中，被丢弃
		"quick.orange.male.rabbit", // 长度为4，都未命中，被丢弃
		"lazy.orange.male.rabbit",  // 被通配符Q2命中
	}

	p := PublishMQ.NewTopicPublishMQ(excahngeName, routingKeys, durable, noWait)
	for _, r := range routingKeys {
		if routingKey := p.GetRoutingKey(r); routingKey != "" {
			p.TopicPublish("", expiration, routingKey)
			time.Sleep(7 * time.Second)
		}
	}
}

// 队列上1个RoutingKey, 2个消费者
func TestTopicC1(t *testing.T) {
	excahngeName := "topic-mode-exchange-test"
	queueName := "topic-mode-queue-test1"
	routingKeys := []string{"*.orange.*"}
	prefetchCount := 1
	durable := false
	noWait := false
	openDeadQueue := false
	c := ConsumeMQ.NewTopicConsumeMQ(
		excahngeName,
		queueName,
		routingKeys,
		prefetchCount,
		durable,
		noWait,
		openDeadQueue,
	)
	msgChan, err := c.TopicChan()
	if err != nil {
		log.Println(err)
		return
	}

	for m := range msgChan {
		log.Printf("%s 被 %s 命中\n", routingKeys, string(m.RoutingKey))
		time.Sleep(time.Millisecond * 200)
		c.Ack(m.DeliveryTag, false) // 单条应答
	}
}

// 队列上1个RoutingKey, 2个消费者
func TestTopicC2(t *testing.T) {
	excahngeName := "topic-mode-exchange-test"
	queueName := "topic-mode-queue-test1"
	routingKeys := []string{"*.orange.*"}
	prefetchCount := 1
	durable := false
	noWait := false
	openDeadQueue := false
	c := ConsumeMQ.NewTopicConsumeMQ(
		excahngeName,
		queueName,
		routingKeys,
		prefetchCount,
		durable,
		noWait,
		openDeadQueue,
	)
	msgChan, err := c.TopicChan()
	if err != nil {
		log.Println(err)
		return
	}

	for m := range msgChan {
		log.Printf("%s 被 %s 命中\n", routingKeys, string(m.RoutingKey))
		time.Sleep(time.Millisecond * 200)
		c.Ack(m.DeliveryTag, false) // 单条应答
	}
}

// 队列上2个RoutingKey, 1个消费者
func TestTopicC3(t *testing.T) {
	excahngeName := "topic-mode-exchange-test"
	queueName := "topic-mode-queue-test2"
	routingKeys := []string{"*.*.rabbit", "lazy.#"}
	prefetchCount := 1
	durable := false
	noWait := false
	openDeadQueue := false
	c := ConsumeMQ.NewTopicConsumeMQ(
		excahngeName,
		queueName,
		routingKeys,
		prefetchCount,
		durable,
		noWait,
		openDeadQueue,
	)
	msgChan, err := c.TopicChan()
	if err != nil {
		log.Println(err)
		return
	}

	for m := range msgChan {
		log.Printf("%s 被 %s 命中\n", routingKeys, string(m.RoutingKey))
		time.Sleep(time.Millisecond * 200)
		c.Ack(m.DeliveryTag, false) // 单条应答
	}
}
