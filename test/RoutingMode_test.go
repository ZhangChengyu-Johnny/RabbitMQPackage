package test

import (
	"RabbitMQPackage/ConsumeMQ"
	"RabbitMQPackage/PublishMQ"
	"fmt"
	"log"
	"testing"
	"time"
)

func TestRoutingP(t *testing.T) {
	p := PublishMQ.NewRoutingPublishMQ("routing-mode-exchange-test", false, false)
	routingKey := []string{"info", "warning", "error"}
	for i := 0; i < 1000; i++ {
		msg := fmt.Sprintf(
			"RoutingKey: %s, 当前时间:%s, 这是第%d条消息",
			routingKey[i%len(routingKey)],
			time.Now().Format("2006-01-02 15:04:05"),
			i,
		)
		if err := p.DirectPublish(msg, routingKey[i%len(routingKey)]); err != nil {
			log.Println(err)
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	time.Sleep(1 * time.Second)

}

func TestRoutingC1(t *testing.T) {
	exchangeName := "routing-mode-exchange-test"
	queueName := "routing-mode-queue-test1"
	routingKey := "info"
	prefetchCount := 100
	durable := false
	noWait := false
	ackCounter := 0

	c := ConsumeMQ.NewRoutingConsumeMQ(
		exchangeName,
		queueName,
		routingKey,
		prefetchCount,
		durable,
		noWait,
	)

	msgChan, err := c.RoutingChan()
	if err != nil {
		log.Println(err)
		return
	}

	for msg := range msgChan {
		ackCounter++
		fmt.Println(string(msg.Body))
		if ackCounter >= 100 {
			c.Ack(msg.DeliveryTag, true)
			ackCounter = 0
		}
	}

}

func TestRoutingC2(t *testing.T) {
	exchangeName := "routing-mode-exchange-test"
	queueName := "routing-mode-queue-test2"
	routingKey := "warning"
	prefetchCount := 100
	durable := false
	noWait := false
	ackCounter := 0

	c := ConsumeMQ.NewRoutingConsumeMQ(
		exchangeName,
		queueName,
		routingKey,
		prefetchCount,
		durable,
		noWait,
	)

	msgChan, err := c.RoutingChan()
	if err != nil {
		log.Println(err)
		return
	}

	for msg := range msgChan {
		ackCounter++
		fmt.Println(string(msg.Body))
		if ackCounter >= 100 {
			c.Ack(msg.DeliveryTag, true)
			ackCounter = 0
		}
	}

}

func TestRoutingC3(t *testing.T) {
	exchangeName := "routing-mode-exchange-test"
	queueName := "routing-mode-queue-test3"
	routingKey := "error"
	prefetchCount := 100
	durable := false
	noWait := false
	ackCounter := 0

	c := ConsumeMQ.NewRoutingConsumeMQ(
		exchangeName,
		queueName,
		routingKey,
		prefetchCount,
		durable,
		noWait,
	)

	msgChan, err := c.RoutingChan()
	if err != nil {
		log.Println(err)
		return
	}

	for msg := range msgChan {
		ackCounter++
		fmt.Println(string(msg.Body))
		if ackCounter >= 100 {
			c.Ack(msg.DeliveryTag, true)
			ackCounter = 0
		}
	}

}
