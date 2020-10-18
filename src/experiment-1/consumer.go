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

	fmt.Println("Pulsar Consumer")

	// Configuration variables pertaining to this consumer
	tokenStr, _ := os.LookupEnv("KESQUE_TOKEN")
	uri,_ := os.LookupEnv("KESQUE_URI")
	topicName,_ := os.LookupEnv("KESQUE_TOPIC")
	subscriptionName := "my-subscription"

	token := pulsar.NewAuthenticationToken(tokenStr)

	// Pulsar client
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:                   uri,
		Authentication:        token,
		//TLSTrustCertsFilePath: trustStore,
	})

	if err != nil {
		log.Fatal(err)
	}

	defer client.Close()

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: subscriptionName,
	})

	if err != nil {
		log.Fatal(err)
	}

	defer consumer.Close()

	ctx := context.Background()

	// infinite loop to receive messages
	for {
		msg, err := consumer.Receive(ctx)
		if err != nil {
			log.Fatal(err)
		} else {
			fmt.Println("Received message : ", string(msg.Payload()))
		}

		consumer.Ack(msg)
	}

}