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
	p := PublishMQ.NewSubscriptionPublishMQ("subscription-mode-exchange-test", false, false)

	for i := 0; i < 10; i++ {
		msg := fmt.Sprintf("当前时间:%s, 这是第%d条消息", time.Now().Format("2006-01-02 15:04:05"), i)

		if err := p.FanoutPublish(msg); err != nil {
			log.Println(err)
			return
		}
	}
	time.Sleep(1 * time.Second)
}

func TestSubscriptionC1(t *testing.T) {
	exChangeName := "subscription-mode-exchange-test"
	prefetchCount := 1 // 按性能转发
	durable := false
	noWait := false
	c := ConsumeMQ.NewSubscriptionConsumeMQ(exChangeName, prefetchCount, durable, noWait)

	msgChan, err := c.FanoutChan()
	if err != nil {
		log.Println(err)
		return
	}
	for msg := range msgChan {
		fmt.Println(string(msg.Body))
		c.Ack(msg.DeliveryTag, false) // 单条确认

	}
}

func TestSubscriptionC2(t *testing.T) {
	exChangeName := "subscription-mode-exchange-test"
	prefetchCount := 100 // 按性能转发
	durable := false
	noWait := false
	ackCounter := 0
	c := ConsumeMQ.NewSubscriptionConsumeMQ(exChangeName, prefetchCount, durable, noWait)

	msgChan, err := c.FanoutChan()
	if err != nil {
		log.Println(err)
		return
	}

	for msg := range msgChan {
		ackCounter++
		fmt.Println(string(msg.Body))
		if ackCounter >= 15 { // 每15条批量应答
			c.Ack(msg.DeliveryTag, true)
			ackCounter = 0
		}
	}

}
