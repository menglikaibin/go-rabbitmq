package RabbitMQ

import (
	"amqp"
	"fmt"
	"log"
)

// url 格式 amqp://账号:密码@rabbitmq服务器地址:端口号/vhost
const MQURL = "amqp://imoocuser:imoocuser@127.0.0.1:15672/imooc"

type RabbitMQ struct {
	conn *amqp.Connection
	channel *amqp.Channel
	// 队列名称
	QueueName string
	// 交换机
	Exchange string
	// key
	Key string
	// 链接信息
	MqUrl string
}

// 创建结构体实例
func newRabbitMQ(queueName string, exchange string, key string) *RabbitMQ {
	rabbitmq := &RabbitMQ{QueueName: queueName, Exchange: exchange, Key: key, MqUrl: MQURL}

	var err error

	// 创建rabbitmq链接
	rabbitmq.conn, err = amqp.Dial(rabbitmq.MqUrl)
	rabbitmq.failOnErr(err, "创建链接错误")

	rabbitmq.channel, err = rabbitmq.conn.Channel()
	rabbitmq.failOnErr(err, "获取channel失败")

	return rabbitmq
}

// 断开channel
func (r *RabbitMQ) destroy() {
	r.channel.Close()
	r.conn.Close()
}

// 错误处理函数
func (r *RabbitMQ) failOnErr(err error, message string)  {
	if err != nil {
		log.Fatalf("%s:%s", message, err)
		panic(fmt.Sprintf("%s:%s", message, err))
	}
}

// 创建简单模式下rabbitmq实例
func newRabbitMQSimple(queueName string) *RabbitMQ {
	return newRabbitMQ(queueName, "", "")
}

// 简单模式下生产代码
func (r *RabbitMQ) PublishSimple (message string) {
	// 1.申请队列,如果队列不存在会自动创建,如果存在,则跳过创建
	// 保证队列存在,消息能发送到队列中
	_ , err := r.channel.QueueDeclare(
			r.QueueName,
			// 是否持久化
			false,
			// 是否自动删除
			false,
			// 是否具有排他性
			false,
			// 是否阻塞
			false,
			// 额外属性
			nil,
		)
	if err != nil {
		fmt.Println(err)
	}

	// 2. 发送消息到队列中
	r.channel.Publish(
		r.Exchange,
		r.QueueName,
		// 如果为true,根据exchange类型和routkey规则,如果无法找到符合条件的队列,那么会把发送消息返回给发送者
		false,
		// 如果为true,当exchange发送消息到队列后发现队列上没有绑定消费者,则会把消息发还给发送者
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		})
}

// 简单模式下消费
func (r *RabbitMQ) ConsumeSimple() {
	// 1.申请队列,如果队列不存在会自动创建,如果存在,则跳过创建
	// 保证队列存在,消息能发送到队列中
	_, err := r.channel.QueueDeclare(
		r.QueueName,
		// 是否持久化
		false,
		// 是否自动删除
		false,
		// 是否具有排他性
		false,
		// 是否阻塞
		false,
		// 额外属性
		nil,
	)
	if err != nil {
		fmt.Println(err)
	}

	// 接受消息
	msgs, err := r.channel.Consume(
		r.QueueName,
		// 用来区分多个消费者
		"",
		// 是否自动应答
		true,
		// 是否具有排他性
		false,
		// true:不能将同一个connection中发送的消息传递给这个connection中的消费者
		false,
		// 队列是否设置为阻塞
		false,
		nil,
	)

	if err != nil {
		fmt.Println(err)
	}

	forever := make(chan bool)
	// 启用协程处理消息
	go func() {
		for d := range msgs {
			// 实现我们要处理的逻辑函数
			log.Printf("Received a message: %s\n", d.Body)
			fmt.Println(d.Body)
		}
	}()

	log.Println("[*] Waiting for messages, To exit press CTRL + C")
	<-forever
}