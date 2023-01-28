package test

import (
	"RabbitMQPackage/ConsumeMQ"
	"RabbitMQPackage/PublishMQ"
	"fmt"
	"log"
	"testing"
	"time"
)

func TestSubscriptionP(t *testing.T) {
	p := PublishMQ.NewSubscriptionPublishMQ("subscription-mode-exchange-test", false, false, nil)

	for i := 0; i < 1000; i++ {
		msg := fmt.Sprintf("当前时间:%s, 这是第%d条消息", time.Now().Format("2006-01-02 15:04:05"), i)
		if err := p.SubscriptionPublishMessage(msg); err != nil {
			log.Println(err)
			return
		}
	}
}

func TestSubscriptionC1(t *testing.T) {
	var ackCounter, batchSize int = 0, 5
	c := ConsumeMQ.NewSubscriptionConsumeMQ("subscription-mode-exchange-test", batchSize, false, false, nil)

	msgChan, err := c.SubscriptionMessageChan(false, nil)
	if err != nil {
		log.Println(err)
		return
	}

	for msg := range msgChan {
		ackCounter++
		fmt.Println(string(msg.Body))
		time.Sleep(time.Millisecond * 200)
		// 每5条确认一次
		if ackCounter >= 5 {
			c.Ack(msg.DeliveryTag, true)
			ackCounter = 0
		}
	}

}

func TestSubscriptionC2(t *testing.T) {
	var ackCounter, batchSize int = 0, 15
	c := ConsumeMQ.NewSubscriptionConsumeMQ("subscription-mode-exchange-test", batchSize, false, false, nil)

	msgChan, err := c.SubscriptionMessageChan(false, nil)
	if err != nil {
		log.Println(err)
		return
	}

	for msg := range msgChan {
		ackCounter++
		fmt.Println(string(msg.Body))
		time.Sleep(time.Millisecond * 200)
		// 每15条确认一次
		if ackCounter >= 15 {
			c.Ack(msg.DeliveryTag, true)
			ackCounter = 0
		}
	}

}
