package main

import "imooc-rabbitmq/RabbitMQ"

// 消费端2 工作模式起到了负载均衡作用
func main()  {
	rabbitmq := RabbitMQ.NewRabbitMQSimple("" + "imoocSimple")
	rabbitmq.ConsumeSimple()
}
