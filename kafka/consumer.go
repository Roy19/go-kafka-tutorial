package kafka

import (
	"go-kafka-tutorial/dtos"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde/avro"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type Consumer struct{}

func (*Consumer) Do(kafkaConfig kafka.ConfigMap, topic string) {
	kafkaConfig["group.id"] = "go-kafka-tutorial"
	kafkaConfig["auto.offset.reset"] = "earliest"

	schemaRegsitryUrl := kafkaConfig["schema.registry"].(string)
	client, err := schemaregistry.NewClient(schemaregistry.NewConfig(schemaRegsitryUrl))
	if err != nil {
		log.Fatalf("Failed to initialize connection to schema registry, %v", err)
	}
	deserializer, err := avro.NewSpecificDeserializer(client, serde.ValueSerde, avro.NewDeserializerConfig())
	if err != nil {
		log.Fatalf("Failed to initialize value deserializer, %v", err)
	}

	consumer, err := kafka.NewConsumer(&kafkaConfig)
	if err != nil {
		log.Fatal("Failed to attach a consumer to queue", err.Error())
	}
	err = consumer.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		log.Fatal("Failed to subscribe to topics", err.Error())
	}

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	run := true

	for run {
		select {
		case sig := <-sigchan:
			log.Printf("Caught signal: %v. Terminating\n", sig)
			run = false
		default:
			ev := consumer.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				value := dtos.User{}
				err = deserializer.DeserializeInto(*e.TopicPartition.Topic, e.Value, &value)
				if err != nil {
					log.Printf("Failed to deserialize value, %v\n", err)
				} else {
					log.Printf("Key: %v, Value: %v\n", e.Key, value)
				}
			case kafka.Error:
				log.Printf("ERROR: %v: %v\n", e.Code(), e)
			default:
				log.Printf("Ignored %v\n", e)
			}
		}
	}

	consumer.Close()
}
