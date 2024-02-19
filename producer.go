package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/streadway/amqp"
)

// Message strucuture
type NotificationProducer struct {
	Timestamp      	time.Time `json:"timestamp"`
	EventId			string `json:"eventId"`
	NotificationId 	string `json:"notificationId"`
	VehicleId      	string `json:"vehicleId"`
	Data           	string `json:"data"`
}

func main() {

	//Generate UUID
	id := uuid.New()
	uuidStr := id.String()
	
	//Convert id to string
	//Connect to RabbitMQ server
	connection, err := amqp.Dial("amqp://guest:guest@localhost:5672/")

	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
		panic(err)
	}
	defer connection.Close()
	fmt.Println("Successfully connected to RabbitMQ instance")

	//Opening a channel over the connection established to inteact with RabbitMQ
	channel, err := connection.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
		panic(err)
	}
	defer channel.Close()

	//Declaring queue with its properties over the the channel opened
	queue, err := channel.QueueDeclare(
		"NotificationsQueue", //name
		false,          //durable
		false,          //auto delete
		false,          //exclusive
		false,          //noWait
		nil,            //args
	)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
		panic(err)
	}

	//Create a message
	message := NotificationProducer{
		Timestamp: time.Now(),
		EventId: uuidStr,
		NotificationId: "666",
		VehicleId: uuidStr,
		Data: "Accidente reportado en el camion con Id: " + uuidStr,
	}

	//Serialise message to JSON
	messageJSON, err := json.Marshal(message)
	if err != nil {
		log.Fatalf("Failed to marshal JSON: %v", err)

	}

	//Publish a message to a Queue
	err = channel.Publish(
		"",             		//exchange
		"NotificationsQueue", 	//key string
		false,          		//mandatory
		false,          		//inmediate
		amqp.Publishing{
			//ContentType: "text/plain",
			//Body:        []byte("Test message"),
			ContentType: "applicaton/json",
			Body:        messageJSON,
			MessageId:   uuidStr, //use UUID as messageID
		},
	)
	if err != nil {
		log.Fatalf("Failed to publish a message: %v", err)
		panic(err)

	}
	fmt.Println("Queue Status:", queue)
	fmt.Println("Successfully published message: ", message,"timeStamp: ", message.Timestamp)

}
