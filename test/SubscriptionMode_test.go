package test

import (
	"RabbitMQPackage"
	"context"
	"fmt"
	"log"
	"testing"
	"time"
)

func TestSubscriptionP(t *testing.T) {
	mode := RabbitMQPackage.SubscriptionMode
	exchangeName := "subscription-mode-exchange"
	routingKeys := []string{""}
	durable := false
	noWait := false

	contextType := "text/plain"
	priorityLevel := 0
	expiration := 0
	confirm := false

	workP := RabbitMQPackage.NewPublishMQ(mode, exchangeName, routingKeys, durable, noWait, confirm)
	for i := 0; i < 1000; i++ {
		data := fmt.Sprintf("当前时间:%s, 这是第%d条消息", time.Now().Format("2006-01-02 15:04:05"), i)
		msg := workP.CreateMessage(data, contextType, priorityLevel, expiration)
		workP.Publish(msg, routingKeys[0], context.Background())
	}
}

func TestSubscriptionC1(t *testing.T) {
	mode := RabbitMQPackage.SubscriptionMode
	exchangeName := "subscription-mode-exchange"
	queueName := "subscription-mode-queue-1"
	durable := false
	noWait := false
	routingKeys := []string{""}
	prefetchCount := 100
	deadQueue := false

	consumer := RabbitMQPackage.NewConsumeMQ(mode, exchangeName, queueName, routingKeys, durable, noWait, prefetchCount, deadQueue)

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

func TestSubscriptionC2(t *testing.T) {
	mode := RabbitMQPackage.SubscriptionMode
	exchangeName := "subscription-mode-exchange"
	queueName := "subscription-mode-queue-2"
	durable := false
	noWait := false
	routingKeys := []string{""}
	prefetchCount := 100
	deadQueue := false

	consumer := RabbitMQPackage.NewConsumeMQ(mode, exchangeName, queueName, routingKeys, durable, noWait, prefetchCount, deadQueue)

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

func TestSubscriptionC3(t *testing.T) {
	mode := RabbitMQPackage.SubscriptionMode
	exchangeName := "subscription-mode-exchange"
	queueName := "subscription-mode-queue-3"
	durable := false
	noWait := false
	routingKeys := []string{""}
	prefetchCount := 100
	deadQueue := false

	consumer := RabbitMQPackage.NewConsumeMQ(mode, exchangeName, queueName, routingKeys, durable, noWait, prefetchCount, deadQueue)

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
