package test

import (
	"RabbitMQPackage"
	"context"
	"fmt"
	"log"
	"testing"
	"time"
)

func TestWorkP(t *testing.T) {
	mode := RabbitMQPackage.WorkMode
	exchangeName := "work-mode-exchange"
	routingKeys := []string{"work-mode-k1"}
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
		workP.Publish(msg, routingKeys[0], context.Background())
	}
}

func TestWorkC1(t *testing.T) {
	mode := RabbitMQPackage.WorkMode
	exchangeName := "work-mode-exchange"
	queueName := "work-mode-queue-1"
	durable := false
	noWait := false
	routingKeys := []string{"work-mode-k1"}
	prefetchCount := 100
	deadQueue := true

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

func TestWorkC2(t *testing.T) {
	mode := RabbitMQPackage.WorkMode
	exchangeName := "work-mode-exchange-dq-exchange"
	queueName := "work-mode-queue-1-dq-queue"
	durable := true
	noWait := false
	routingKeys := []string{"work-mode-k1-dq-queue"}
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
