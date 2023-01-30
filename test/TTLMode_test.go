package test

import (
	"RabbitMQPackage"
	"context"
	"fmt"
	"log"
	"testing"
	"time"
)

func TestTTLC(t *testing.T) {
	// 配置TTL Consume
	exchangeName := "ttl-enum-mode-exchange"
	queueName := "ttl-enum-mode-q1"
	prefetchCount := 0
	deadQueue := true

	// 配置延迟队列和交换机
	delayExchangeName := "delay-exchange"
	delayQueueExpirationConf := make(map[string]string)
	delayQueueExpirationConf["1squeue"] = "1000"
	delayQueueExpirationConf["5squeue"] = "5000"
	delayQueueExpirationConf["30squeue"] = "30000"

	consumer := RabbitMQPackage.NewTTLConsumeMQNewConsumeMQ(
		exchangeName,
		queueName,
		prefetchCount,
		deadQueue,
		delayExchangeName,
		delayQueueExpirationConf,
	)

	msgChan, err := consumer.MessageChan()
	if err != nil {
		log.Println(err)
		return
	}

	for m := range msgChan {
		fmt.Println("message:", string(m.Body))
		fmt.Println("from:", m.RoutingKey)
		consumer.Ack(m.DeliveryTag, false)
	}
}

// TTL的生产者就是普通Routing
func TestTTLP(t *testing.T) {
	mode := RabbitMQPackage.RoutingMode
	exchangeName := "delay-exchange"
	routingKeys := []string{"1squeue-rk", "5squeue-rk", "30squeue-rk"}
	durable := true
	noWait := false

	contextType := "text/plain"
	priorityLevel := 0
	expiration := 999999 // 设置超大消息延迟时间，用队列设置的
	confirm := false

	ttlP := RabbitMQPackage.NewPublishMQ(mode, exchangeName, routingKeys, durable, noWait, confirm)
	for i := 0; i < 100; i++ {
		data := fmt.Sprintf("当前时间:%s, 这是第%d条消息", time.Now().Format("2006-01-02 15:04:05"), i)
		msg := ttlP.CreateMessage(data, contextType, priorityLevel, expiration)
		ttlP.Publish(msg, routingKeys[i%3], context.Background())
	}
}
