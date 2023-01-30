package test

import (
	"RabbitMQPackage"
	"context"
	"fmt"
	"log"
	"testing"
	"time"
)

func TestConfirmP(t *testing.T) {
	mode := RabbitMQPackage.ConfirmMode
	exchangeName := "confirm-mode-exchange"
	routingKeys := []string{"confirm-mode-k"}
	durable := false
	noWait := false

	contextType := "text/plain"
	priorityLevel := 0
	expiration := 5000
	confirm := true

	confirmP := RabbitMQPackage.NewPublishMQ(mode, exchangeName, routingKeys, durable, noWait, confirm)

	for i := 0; i < 3000; i++ {
		data := fmt.Sprintf("当前时间:%s, 这是第%d条消息", time.Now().Format("2006-01-02 15:04:05"), i)
		msg := confirmP.CreateMessage(data, contextType, priorityLevel, expiration)
		confirmP.Publish(msg, routingKeys[0], context.Background())
	}
	time.Sleep(3 * time.Second)
	nackCount := 0
	confirmP.MessageMap.Range(func(k, v any) bool {
		nackCount++
		return true
	})
	fmt.Println("nack count:", nackCount)
	fmt.Println("ack count:", confirmP.AckCounter)
}

func TestConfirmC(t *testing.T) {
	mode := RabbitMQPackage.ConfirmMode
	exchangeName := "confirm-mode-exchange"
	queueName := "confirm-mode-queue-1"
	durable := false
	noWait := false
	routingKeys := []string{"confirm-mode-k"}
	prefetchCount := 1000
	deadQueue := true

	consumer := RabbitMQPackage.NewConsumMQ(mode, exchangeName, queueName, routingKeys, durable, noWait, prefetchCount, deadQueue)
	msgChan, err := consumer.MessageChan()
	if err != nil {
		log.Println(err)
		return
	}
	for m := range msgChan {
		log.Printf("%s get message: %s\n", queueName, string(m.Body))
		time.Sleep(time.Millisecond * 100)
		consumer.Ack(m.DeliveryTag, false) // 单条应答
	}
}
