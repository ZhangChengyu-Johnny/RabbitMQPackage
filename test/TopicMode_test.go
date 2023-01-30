package test

import (
	"RabbitMQPackage"
	"context"
	"log"
	"testing"
	"time"
)

// // routingKey 1: *.orange.*
// // routingKey 2: *.*.rabbit
// // routingKey 3: lazy.#

func TestTopicP(t *testing.T) {
	mode := RabbitMQPackage.TopicMode
	exchangeName := "topic-mode-exchange"
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
	durable := false
	noWait := false

	contextType := "text/plain"
	priorityLevel := 0
	expiration := 5000
	confirm := true

	topicP := RabbitMQPackage.NewPublishMQ(mode, exchangeName, routingKeys, durable, noWait, confirm)
	for _, r := range routingKeys {
		msg := topicP.CreateMessage(r, contextType, priorityLevel, expiration)
		topicP.Publish(msg, r, context.Background())
		time.Sleep(7 * time.Second)

	}
}

func TestTopicC1(t *testing.T) {
	mode := RabbitMQPackage.TopicMode
	exchangeName := "topic-mode-exchange"
	queueName := "topic-mode-queue-1"
	durable := false
	noWait := false
	routingKeys := []string{"*.orange.*"}
	prefetchCount := 100
	deadQueue := true

	consumer := RabbitMQPackage.NewConsumMQ(mode, exchangeName, queueName, routingKeys, durable, noWait, prefetchCount, deadQueue)

	msgChan, err := consumer.MessageChan()
	if err != nil {
		log.Println(err)
		return
	}
	for m := range msgChan {
		log.Printf("%s get message: %s\n", queueName, string(m.Body))
		time.Sleep(time.Millisecond * 200)
		consumer.Ack(m.DeliveryTag, false) // 单条应答
	}
}

func TestTopicC2(t *testing.T) {
	mode := RabbitMQPackage.TopicMode
	exchangeName := "topic-mode-exchange"
	queueName := "topic-mode-queue-2"
	durable := false
	noWait := false
	routingKeys := []string{"*.*.rabbit", "lazy.#"}
	prefetchCount := 100
	deadQueue := true

	consumer := RabbitMQPackage.NewConsumMQ(mode, exchangeName, queueName, routingKeys, durable, noWait, prefetchCount, deadQueue)

	msgChan, err := consumer.MessageChan()
	if err != nil {
		log.Println(err)
		return
	}
	for m := range msgChan {
		log.Printf("%s get message: %s\n", queueName, string(m.Body))
		time.Sleep(time.Millisecond * 200)
		consumer.Ack(m.DeliveryTag, false) // 单条应答
	}
}
