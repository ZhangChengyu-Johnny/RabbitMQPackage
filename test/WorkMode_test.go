package test

import (
	"RabbitMQPackage/ConsumeMQ"
	"RabbitMQPackage/PublishMQ"
	"fmt"
	"log"
	"testing"
	"time"
)

func TestWorkP(t *testing.T) {
	queues := []string{"work-mode-test1", "work-mode-test2"}
	durable := false
	noWait := false
	// 实例化工作模式的生产者
	p := PublishMQ.NewWorkPublishMQ()
	// 生产者注册路由(工作模式中是注册队列)
	for _, q := range queues {
		err := p.QueueDeclare(q, durable, noWait)
		if err != nil {
			log.Println(err)
			return
		}
	}

	for i := 0; i < 3000; i++ {
		msg := fmt.Sprintf("当前时间:%s, 这是第%d条消息", time.Now().Format("2006-01-02 15:04:05"), i)
		if err := p.DefaultPublish(msg, queues[i%2]); err != nil {
			log.Println(err)
			return
		}
	}
}

func TestWorkC1(t *testing.T) {
	queueName := "work-mode-test1"
	prefetchCount := 1
	durable := false
	noWait := false
	c := ConsumeMQ.NewWorkConsumeMQ(queueName, prefetchCount, durable, noWait)
	msgChan, err := c.DefaultChan()
	if err != nil {
		log.Println(err)
		return
	}

	for m := range msgChan {
		log.Printf("%s get message: %s\n", queueName, string(m.Body))
		time.Sleep(time.Millisecond * 200)
		c.Ack(m.DeliveryTag, false) // 单条应答
	}
}

func TestWorkC2(t *testing.T) {
	queueName := "work-mode-test1"
	prefetchCount := 1
	durable := false
	noWait := false
	c := ConsumeMQ.NewWorkConsumeMQ(queueName, prefetchCount, durable, noWait)
	msgChan, err := c.DefaultChan()
	if err != nil {
		log.Println(err)
		return
	}

	for m := range msgChan {
		log.Printf("%s get message: %s\n", queueName, string(m.Body))
		time.Sleep(time.Millisecond * 200)
		c.Ack(m.DeliveryTag, false) // 单条应答
	}

}

func TestWorkC3(t *testing.T) {
	queueName := "work-mode-test2"
	prefetchCount := 10
	durable := false
	noWait := false
	ackCounter := 0
	c := ConsumeMQ.NewWorkConsumeMQ(queueName, prefetchCount, durable, noWait)
	msgChan, err := c.DefaultChan()
	if err != nil {
		log.Println(err)
		return
	}

	for m := range msgChan {
		ackCounter++
		log.Printf("%s get message: %s\n", queueName, string(m.Body))
		time.Sleep(time.Millisecond * 100)
		if ackCounter >= 10 {
			ackCounter = 0
			c.Ack(m.DeliveryTag, true) // 批量应答
		}

	}

}
