package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/streadway/amqp"
)

type Supplier struct {
	TimestampConsumed  time.Time     `json:"timestampConsumed"`
	TimestampPublished time.Time     `json:"timestampPublished"`
	TimestampGMTDif    time.Duration `json:"timestampGMTDif"`
	EventId            string        `json:"eventId"`
	NotificationId     string        `json:"notificationId"`
	VehicleId          string        `json:"vehicleId"`
	Email              string        `json:"email"`
	Phone              string        `json:"phone"`
	Data               string        `json:"data"`
}

func main() {

	fmt.Println("Consumer Application")

	//1. Connect to RabbitMQ server
	fmt.Println("Connecting to RabbitMQ...:")
	rabbitMQURL := "amqp://guest:guest@localhost:5672/"

	connection, err := amqp.Dial(rabbitMQURL)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
		panic(err)
	}
	defer connection.Close()
	fmt.Println("Succesfully connected to RabbitMQ")

	//2.Open a channel over the connection established to inteact with RabbitMQ
	channel, err := connection.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel in RabbitMQ: %v", err)
		panic(err)
	}
	defer channel.Close()

	//3. Declare suppliers Queue
	SuppliersQueue, err := channel.QueueDeclare(
		"SuppliersQueue", //name
		false,            //durable
		false,            //auto delete
		false,            //exclusive
		false,            //noWait
		nil,              //args
	)
	if err != nil {
		log.Fatalf("Failed to declare a queue SuppliersQueue: %v", SuppliersQueue, err)
		panic(err)
	}

	//4. Declare File
	file, err := os.Create("output.txt")
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)

	}
	defer file.Close()

	//5. Consume messages from te queue
	msgs, err := channel.Consume(
		"SuppliersQueue", //Queue name
		"",               //Consumer
		false,            //AutoAck
		false,            //exclusive
		false,            //no local
		false,            //nowait
		nil,              //args
	)
	if err != nil {
		log.Fatalf("Failed to register a consumer: %v", err)
		panic(err)
	}

	// Setup signal handler for graceful shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	//4. Loop to read Queue & write file
	for msg := range msgs {
		//Write message to file
		_, err := file.WriteString(fmt.Sprintf("%s\n", msg.Body))
		if err != nil {
			log.Fatalf("Failed to write message to file: %v", err)
		}
	}
	// Wait for termination signal
	<-sig
	log.Println("Shutting down...")

}
