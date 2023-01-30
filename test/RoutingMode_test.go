package test

import (
	"RabbitMQPackage"
	"context"
	"fmt"
	"log"
	"testing"
	"time"
)

func TestRoutingP(t *testing.T) {
	mode := RabbitMQPackage.RoutingMode
	exchangeName := "routing-mode-exchange"
	routingKeys := []string{"routing-mode-k1", "routing-mode-k2"}
	durable := false
	noWait := false

	contextType := "text/plain"
	priorityLevel := 0
	expiration := 5000
	confirm := false

	workP := RabbitMQPackage.NewPublishMQ(mode, exchangeName, routingKeys, durable, noWait, confirm)
	for i := 0; i < 3000; i++ {
		data := fmt.Sprintf("当前时间:%s, 这是第%d条消息", time.Now().Format("2006-01-02 15:04:05"), i)
		msg := workP.CreateMessage(data, contextType, priorityLevel, expiration)
		workP.Publish(msg, routingKeys[i%2], context.Background())
	}
}

func TestRoutingC1(t *testing.T) {
	mode := RabbitMQPackage.RoutingMode
	exchangeName := "routing-mode-exchange"
	queueName := "routing-mode-queue-1"
	durable := false
	noWait := false
	routingKeys := []string{"routing-mode-k1"}
	prefetchCount := 100
	deadQueue := false

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

func TestRoutingC2(t *testing.T) {
	mode := RabbitMQPackage.RoutingMode
	exchangeName := "routing-mode-exchange"
	queueName := "routing-mode-queue-1"
	durable := false
	noWait := false
	routingKeys := []string{"routing-mode-k1"}
	prefetchCount := 100
	deadQueue := false

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

func TestRoutingC3(t *testing.T) {
	mode := RabbitMQPackage.RoutingMode
	exchangeName := "routing-mode-exchange"
	queueName := "routing-mode-queue-2"
	durable := false
	noWait := false
	routingKeys := []string{"routing-mode-k2"}
	prefetchCount := 100
	deadQueue := false

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
