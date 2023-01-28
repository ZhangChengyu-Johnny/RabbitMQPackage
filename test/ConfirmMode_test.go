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
	p := PublishMQ.NewConfirmPublishMQ()
	err := p.QueueDeclare(queueName, false, false, nil)
	if err != nil {
		log.Println(err)
		return
	}

	for i := 0; i < 1000000; i++ {
		msg := fmt.Sprintf("当前时间:%s, 这是第%d条消息", time.Now().Format("2006-01-02 15:04:05"), i)
		if err = p.ConfirmPublishMessage(msg, queueName); err != nil {
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
	c := ConsumeMQ.NewWorkConsumeMQ(1000, 0)

	err := c.QueueDeclare(queueName, false, false, nil)
	if err != nil {
		log.Println(err)
		return
	}

	msgChan, err := c.WorkMessageChan(queueName, false, nil)
	if err != nil {
		log.Println(err)
		return
	}

	var ackCounter int
	for msg := range msgChan {
		ackCounter++
		fmt.Println(string(msg.Body))
		if ackCounter >= 100 {
			c.Ack(msg.DeliveryTag, true)
			ackCounter = 0
		}
	}

}

func TestConfirmC2(t *testing.T) {
	queueName := "confirm-mode-test1"
	c := ConsumeMQ.NewWorkConsumeMQ(1000, 0)

	err := c.QueueDeclare(queueName, false, false, nil)
	if err != nil {
		log.Println(err)
		return
	}

	msgChan, err := c.WorkMessageChan(queueName, false, nil)
	if err != nil {
		log.Println(err)
		return
	}

	var ackCounter int
	for msg := range msgChan {
		ackCounter++
		fmt.Println(string(msg.Body))
		if ackCounter >= 100 {
			c.Ack(msg.DeliveryTag, true)
			ackCounter = 0
		}
	}

}
