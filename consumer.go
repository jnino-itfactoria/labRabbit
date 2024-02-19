package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type NotificationConsumer struct {
	Timestamp      time.Time `json:"timestamp"`
	EventId        string `json:"eventId"`
	NotificationId string `json:"notificationId"`
	VehicleId      string `json:"vehicleId"`
	Data           string `json:"data"`
}

type NotificationRule struct {
	Id         string `bson:"id"`
	Name       string `bson:"name"`
	Priority   string `bson:"priority"`
	Recipients string `bson:"recipients"`
}

type Supplier struct {
	TimestampConsumed	time.Time `json:"timestampConsumed"`
	TimestampPublished 	time.Time `json:"timestampPublished"`
	TimestampGMTDif 	time.Duration `json:"timestampGMTDif"`
	EventId        		string `json:"eventId"`
	NotificationId 		string `json:"notificationId"`
	VehicleId      		string `json:"vehicleId"`
	Email          		string `json:"email"`
	Phone          		string `json:"phone"`
	Data           		string `json:"data"`
}

func main() {

	fmt.Println("Consumer Application")
	var timeStamp string = time.Now().String()

	//1. Conect to MongoDB
	fmt.Println("Connecting to MongoDB...:", timeStamp)
	mongoURI := "mongodb://localhost:27017"
	clientOptions := options.Client().ApplyURI(mongoURI)

	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		log.Fatalf("Failed to connect MongoDB: %v", err)
	}
	defer client.Disconnect(context.Background())

	//1.1.. Connect to Database
	database := client.Database("test")

	//1.2 Accesing a collection
	collection := database.Collection("notificationsRules")
	fmt.Println("Succesfully connected to MongoDB: ", timeStamp)

	//2. Connect to RabbitMQ server
	fmt.Println("Connecting to RabbitMQ...:", timeStamp)
	rabbitMQURL := "amqp://guest:guest@localhost:5672/"

	connection, err := amqp.Dial(rabbitMQURL)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
		panic(err)
	}
	defer connection.Close()
	fmt.Println("Succesfully connected to RabbitMQ", timeStamp)

	//2.1. Open a channel over the connection established to inteact with RabbitMQ
	channel, err := connection.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel in RabbitMQ: %v", err)
		panic(err)
	}
	defer channel.Close()

	//2.2. Declare nofiticationes Queue
	msgs, err := channel.Consume(
		"NotificationsQueue", //Queue name
		"",                   //Consumer
		false,                //AutoAck
		false,                //exclusive
		false,                //no local
		false,                //nowait
		nil,                  //args
	)
	if err != nil {
		log.Fatalf("Failed to declare a queue NotificationsQueue: %v", err)
		panic(err)
	}

	//2.3. Declare suppliers Queue
	suppliersQueue, err := channel.QueueDeclare(
		"SuppliersQueue", //name
		false,            //durable
		false,            //auto delete
		false,            //exclusive
		false,            //noWait
		nil,              //args
	)
	if err != nil {
		log.Fatalf("Failed to declare a queue SuppliersQueue: %v", err)
		panic(err)
	}

	//3.Loop to consume events in NotificationsQueue, Query in MongoDB and publiss messages in SuppliersQueue
	forever := make(chan bool)
	go func() {
		for msg := range msgs {
			fmt.Printf("Received message: %s\n", msg.Body)

			var notification NotificationConsumer
			var result NotificationRule
			var timeStamp = time.Now().String()

			if err := json.Unmarshal(msg.Body, &notification); err != nil {
				log.Printf("Failed to unmarshall message: %v", err)
				continue
			}
			fmt.Printf("Received event: %s\n", notification)


			//Define filter to query MongoDB
			filter := bson.M{"priority": "5"}
			
			//err = collection.FindOne(context.Background(), filter).Decode(&result)
			//err = collection.FindOne(context.Background(), bson.M{"notificationId": notification.NotificationId}).Decode(&result)
			err = collection.FindOne(context.TODO(), filter).Decode(&result)

			if err != nil {
				if err == mongo.ErrNoDocuments {
					log.Printf("Failed to find document in Database: %v", err, "timestamp:", timeStamp)
					//continue

					//******************************
					timeDiference := time.Now().Sub(notification.Timestamp)
					fmt.Println("timediference: ", timeDiference)


					//Create a eventSupplier to pubish event in SuppliesQuee
					eventSupplier := Supplier{
						TimestampConsumed:  notification.Timestamp,
						TimestampPublished: time.Now(),
						TimestampGMTDif: timeDiference,
						EventId:        notification.EventId,
						NotificationId: "666",
						VehicleId:      notification.VehicleId,
						Email:          "policia@email.com",
						Phone:          "301124578",
						Data:           notification.Data,
					}
					//Serialise message to JSON
					eventSupplierJSON, err := json.Marshal(eventSupplier)
					if err != nil {
						log.Fatalf("Failed to marshal JSON: %v", err)
					}
					
					//Publish a message to a Suppliers Queue
					err = channel.Publish(
						"",               //exchange
						"SuppliersQueue", //key string
						false,            //mandatory
						false,            //inmediate
						amqp.Publishing{
							ContentType: "applicaton/json",
							Body:        eventSupplierJSON,
							//MessageId: uuidStr, //use UUID as messageID
						},
					)
					if err != nil {
						log.Fatalf("Failed to publish a event in Suppliers Queue: %v", err)
						panic(err)
					}
					fmt.Println("Queue Status:", suppliersQueue)
					fmt.Println("Successfully published message:",eventSupplier ,timeStamp)

					//****************************

				}
			}

			// Process the retrieved message
			log.Printf("Found document in MongoDB: %v", result)

		}
	}()
	fmt.Println("Waiting for messages")
	<-forever

}
