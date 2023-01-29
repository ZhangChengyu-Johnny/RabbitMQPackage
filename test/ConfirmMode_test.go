package test

import (
	"RabbitMQPackage/ConsumeMQ"
	"RabbitMQPackage/PublishMQ"
	"fmt"
	"log"
	"testing"
	"time"
)

func TestConfirmP(t *testing.T) {
	queueName := "confirm-mode-test1"
	durable := false
	noWait := false
	p := PublishMQ.NewConfirmPublishMQ()
	err := p.QueueDeclare(queueName, durable, noWait)
	if err != nil {
		log.Println(err)
		return
	}

	for i := 0; i < 1000000; i++ {
		msg := fmt.Sprintf("当前时间:%s, 这是第%d条消息", time.Now().Format("2006-01-02 15:04:05"), i)
		if err = p.DefaultPublish(msg, queueName); err != nil {
			log.Println(err)
			return
		}
	}
	time.Sleep(time.Second * 3)

	p.MessageMap.Range(func(k, v any) bool {
		fmt.Println(k, v)
		return true
	})

}

func TestConfirmC1(t *testing.T) {
	queueName := "confirm-mode-test1"
	prefetchCount := 1000 // 轮询分发
	durable := false
	noWait := false
	ackCounter := 0 // 消费计数器
	c := ConsumeMQ.NewWorkConsumeMQ(queueName, prefetchCount, durable, noWait)
	msgChan, err := c.DefaultChan()
	if err != nil {
		log.Println(err)
		return
	}

	for msg := range msgChan {
		ackCounter++
		fmt.Println(string(msg.Body))
		if ackCounter >= 100 { // 批量应答
			c.Ack(msg.DeliveryTag, true)
			ackCounter = 0
		}
	}

}

func TestConfirmC2(t *testing.T) {
	queueName := "confirm-mode-test2"
	prefetchCount := 10 // 轮询分发
	durable := false
	noWait := false
	ackCounter := 0 // 消费计数器
	c := ConsumeMQ.NewWorkConsumeMQ(queueName, prefetchCount, durable, noWait)

	msgChan, err := c.DefaultChan()
	if err != nil {
		log.Println(err)
		return
	}

	for msg := range msgChan {
		ackCounter++
		fmt.Println(string(msg.Body))
		if ackCounter >= 10 { // 批量应答
			c.Ack(msg.DeliveryTag, true)
			ackCounter = 0
		}
	}

}
