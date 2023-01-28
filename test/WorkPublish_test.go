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
	queueName := "work-mode-test1"
	// 实例化工作模式的生产者
	p := PublishMQ.NewWorkPublishMQ()
	// 生产者注册路由(工作模式中是注册队列)
	err := p.QueueDeclare(queueName, false, false, false, false, nil)
	if err != nil {
		log.Println(err)
		return
	}

	for i := 0; i < 4; i++ {
		msg := fmt.Sprintf("当前时间:%s, 这是第%d条消息", time.Now().Format("2006-01-02 15:04:05"), i)
		if err = p.WorkPublishMessage(msg, queueName, false, false); err != nil {
			log.Println(err)
			return
		}
	}
}

func TestWorkC1(t *testing.T) {
	queueName1 := "work-mode-test1"
	queueName2 := "work-mode-test2"
	c := ConsumeMQ.NewWorkConsumeMQ(1, 0)

	err := c.QueueDeclare(queueName1, false, false, false, false, nil)
	if err != nil {
		log.Println(err)
		return
	}
	err = c.QueueDeclare(queueName2, false, false, false, false, nil)
	if err != nil {
		log.Println(err)
		return
	}

	msgChan1, err := c.WorkMessageChan("C1-A", queueName1, false, false, false, false, nil)
	if err != nil {
		log.Println(err)
		return
	}
	msgChan2, err := c.WorkMessageChan("C1-B", queueName2, false, false, false, false, nil)
	if err != nil {
		log.Println(err)
		return
	}

	for {

		select {
		case m1 := <-msgChan1:
			log.Println("Chan1 get message:", string(m1.Body))
			time.Sleep(time.Millisecond * 500)
			c.Ack(m1.DeliveryTag, false)
		case m2 := <-msgChan2:
			log.Println("Chan2 get message:", string(m2.Body))
			time.Sleep(time.Millisecond * 500)
			c.Ack(m2.DeliveryTag, false)
		}
	}
}

func TestWorkC2(t *testing.T) {
	queueName1 := "work-mode-test1"
	queueName2 := "work-mode-test2"
	msgAckBatch, msgCounter1, msgCounter2 := 5, 0, 0
	// 更改分配模式，每次往队列里推5个消息
	c := ConsumeMQ.NewWorkConsumeMQ(msgAckBatch, 0)
	err := c.QueueDeclare(queueName1, false, false, false, false, nil)
	if err != nil {
		log.Println(err)
		return
	}
	err = c.QueueDeclare(queueName2, false, false, false, false, nil)
	if err != nil {
		log.Println(err)
		return
	}

	msgChan1, err := c.WorkMessageChan("C2-A", queueName1, false, false, false, false, nil)
	if err != nil {
		log.Println(err)
		return
	}
	msgChan2, err := c.WorkMessageChan("C2-B", queueName2, false, false, false, false, nil)
	if err != nil {
		log.Println(err)
		return
	}

	for {
		select {
		case m1 := <-msgChan1:
			log.Println("Chan1 get message:", string(m1.Body))
			time.Sleep(time.Second * 1)
			msgCounter1 += 1
			if msgCounter1 >= msgAckBatch {
				c.Ack(m1.DeliveryTag, true)
				msgCounter1 = 0
			}

		case m2 := <-msgChan2:
			log.Println("Chan2 get message:", string(m2.Body))
			time.Sleep(time.Millisecond * 500)
			msgCounter2 += 1
			if msgCounter2 >= msgAckBatch {
				c.Ack(m2.DeliveryTag, true)
				msgCounter1 = 0
			}
		}
	}
}
