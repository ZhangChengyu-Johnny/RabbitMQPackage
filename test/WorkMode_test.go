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
	queueName := "work-mode-test2"
	// 实例化工作模式的生产者
	p := PublishMQ.NewWorkPublishMQ()
	// 生产者注册路由(工作模式中是注册队列)
	err := p.QueueDeclare(queueName, false, false, nil)
	if err != nil {
		log.Println(err)
		return
	}

	for i := 0; i < 3000; i++ {
		msg := fmt.Sprintf("当前时间:%s, 这是第%d条消息", time.Now().Format("2006-01-02 15:04:05"), i)
		if err = p.WorkPublishMessage(msg, queueName); err != nil {
			log.Println(err)
			return
		}
	}
}

func TestWorkC1(t *testing.T) {
	queueName1 := "work-mode-test1"
	queueName2 := "work-mode-test2"
	// 在同一个信道开多个队列不能使用批量接收
	c := ConsumeMQ.NewWorkConsumeMQ(1, 0)

	err := c.QueueDeclare(queueName1, false, false, nil)
	if err != nil {
		log.Println(err)
		return
	}
	err = c.QueueDeclare(queueName2, false, false, nil)
	if err != nil {
		log.Println(err)
		return
	}

	msgChan1, err := c.DefaultChan(queueName1, false, nil)
	if err != nil {
		log.Println(err)
		return
	}
	msgChan2, err := c.DefaultChan(queueName2, false, nil)
	if err != nil {
		log.Println(err)
		return
	}

	for {

		select {
		case m1 := <-msgChan1:
			log.Println("Chan1 get message:", string(m1.Body))
			c.Ack(m1.DeliveryTag, false)
		case m2 := <-msgChan2:
			log.Println("Chan2 get message:", string(m2.Body))
			c.Ack(m2.DeliveryTag, false)
		}
	}
}

func TestWorkC2(t *testing.T) {
	queueName := "work-mode-test1"
	// 更改分配模式，每次往队列里推5个消息
	var batchAck, ackCounter int = 5, 0
	c := ConsumeMQ.NewWorkConsumeMQ(batchAck, 0)

	err := c.QueueDeclare(queueName, false, false, nil)
	if err != nil {
		log.Println(err)
		return
	}

	msgChan, err := c.DefaultChan(queueName, false, nil)
	if err != nil {
		log.Println(err)
		return
	}

	for m := range msgChan {
		log.Println("Chan2 get message:", string(m.Body))
		time.Sleep(time.Millisecond * 500)
		ackCounter += 1
		if ackCounter >= batchAck {
			ackCounter = 0
			c.Ack(m.DeliveryTag, true)
		}
	}

}
