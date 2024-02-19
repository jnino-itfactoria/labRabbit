package main

import (
	"fmt"

	"github.com/streadway/amqp"
)

func main() {
	fmt.Println("RabbitMQ in Golang: Getting started tutorial")

	connection, err := amqp.Dial("amqp://guest:guest@localhost:5672/")

	if err != nil {
		panic(err)

	}
	defer connection.Close()
	fmt.Println("Successfully connected to RabbitMQ instance")

	//Opening a channel over the connection established to inteact with RabbitMQ
	channel, err := connection.Channel()
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
	defer channel.Close()

	//Declaring queue with its properties over the the channel opened
	queue, err := channel.QueueDeclare(
		"testing", //name
		false,     //durable
		false,     //auto delete
		false,     //exclusive
		false,     //noWait
		nil,       //args
	)
	if err != nil {
		panic(err)
	}

	//Publishing a message
	//now := time.Now()
	err = channel.Publish(
		"",        //exchange
		"testing", //key string
		false,     //mandatory
		false,     //inmediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte("Test message"),
		},
	)
	if err != nil {
		panic(err)

	}
	fmt.Println("Queue Status:", queue)
	fmt.Println("Successfully published message")

}
