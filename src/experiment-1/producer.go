package main
import (
	"context"
	"fmt"
	"github.com/joho/godotenv"
	"github.com/shivasishdas/sample-pulsar-go-client/src"
	"log"
	"os"

	"github.com/apache/pulsar/pulsar-client-go/pulsar"
)

func main() {

	// loads values from .env into the system
	if err := godotenv.Load(src.EnvFilePath); err != nil {
		panic("No .env file found")
	}

	fmt.Println("Pulsar Producer")

	// Configuration variables pertaining to this consumer
	tokenStr, _ := os.LookupEnv("KESQUE_TOKEN")
	uri,_ := os.LookupEnv("KESQUE_URI")
	topicName,_ := os.LookupEnv("KESQUE_TOPIC")

	token := pulsar.NewAuthenticationToken(tokenStr)

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:                     uri,
		Authentication:          token,
		//TLSTrustCertsFilePath:   trustStore,
		IOThreads:               3,
		OperationTimeoutSeconds: 5,
	})

	if err != nil {
		log.Fatalf("Could not instantiate Pulsar client: %v", err)
	}

	defer client.Close()

	log.Printf("creating producer...")

	// Use the client to instantiate a producer
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: topicName,
	})

	log.Printf("checking error of producer creation...")
	if producer == nil {
		log.Print("producer is null")
	}
	if err != nil {
		log.Fatal(err)
	}

	defer producer.Close()

	ctx := context.Background()

	// Send 3 messages synchronously and 3 messages asynchronously
	for i := 0; i < 3; i++ {
		// Create a message
		msg := pulsar.ProducerMessage{
			Payload: []byte(fmt.Sprintf("messageId-%d", i)),
		}

		// Attempt to send the message
		if err := producer.Send(ctx, msg); err != nil {
			log.Fatal(err)
		}

		// Create a different message to send asynchronously
		asyncMsg := pulsar.ProducerMessage{
			Payload: []byte(fmt.Sprintf("asyncMessageId-%d", i)),
		}

		// Attempt to send the message asynchronously and handle the response
		producer.SendAsync(ctx, asyncMsg, func(msg pulsar.ProducerMessage, err error) {
			if err != nil {
				log.Fatal(err)
			}

			fmt.Println("successfully published ", string(msg.Payload))
		})
	}
}
