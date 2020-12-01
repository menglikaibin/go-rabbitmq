package main

import "imooc-rabbitmq/RabbitMQ"

// 消费端1
func main()  {
	rabbitmq := RabbitMQ.NewRabbitMQSimple("" + "imoocSimple")
	rabbitmq.ConsumeSimple()
}
