package RabbitMQ

import (
	"amqp"
	"fmt"
	"log"
)

// url 格式 amqp://账号:密码@rabbitmq服务器地址:端口号/vhost
const MQURL = "amqp://imoocuser:imoocuser@localhost:5672/imooc"

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
func NewRabbitMQ(queueName string, exchange string, key string) *RabbitMQ {
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
func NewRabbitMQSimple(queueName string) *RabbitMQ {
	return NewRabbitMQ(queueName, "", "")
}

// 简单模式下生产代码
func (r *RabbitMQ) PublishSimple (message string) {
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

// 订阅模式创建RabbitMQ实例
func NewRabbitMQPubSub (exchangeName string) *RabbitMQ {
	// 创建rabbitmq实例
	rabbitmq := NewRabbitMQ("", exchangeName, "")
	var err error
	// 获取connection
	rabbitmq.conn, err = amqp.Dial(rabbitmq.MqUrl)
	rabbitmq.failOnErr(err, "failed to connect rabbitmq!")
	// 获取channel
	rabbitmq.channel, err = rabbitmq.conn.Channel()
	rabbitmq.failOnErr(err, "fail to open a channel")

	return rabbitmq
}

func (r *RabbitMQ) PublishPub(message string) {
	// 1.尝试创建交换机
	err := r.channel.ExchangeDeclare(
		r.Exchange,
		"fanout",
		true,
		false,
		// true表示这个exchange不可以被client用来推送消息,仅用来进行exchange和exchange之间的绑定
		false,
		false,
		nil,
	)

	r.failOnErr(err, "Failed to declare an exchange")

	// 2.发送消息
	err = r.channel.Publish(
		r.Exchange,
		"",
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:		  []byte(message),
		})
}

// 订阅模式消费端代码
func (r *RabbitMQ) ReceiveSub() {
	// 1.尝试创建交换机
	err := r.channel.ExchangeDeclare(
		r.Exchange,
		// 交换机类型
		"fanout",
		true,
		false,
		false,
		false,
		nil,
	)

	r.failOnErr(err, "fail to dechare as exchange")

	// 2.尝试创建队列,队列名称不要填写
	q, err := r.channel.QueueDeclare(
		"", // 随机生产队列名称
		false,
		false,
		true,
		false,
		nil,
	)

	r.failOnErr(err, "failed to declare a queue")

	// 绑定队列到exchange中
	err = r.channel.QueueBind(
		q.Name,
		// 在pub/sub模式下,这里key要为空,
		"",
		r.Exchange,
		false,
		nil,
	)

	// 消费消息
	messages, err := r.channel.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)

	forever := make(chan bool)

	go func() {
		for d := range messages {
			log.Printf("received a message: %s", d.Body)
		}
	}()

	fmt.Println("退出请按 CTRL + C \n")

	<-forever
}