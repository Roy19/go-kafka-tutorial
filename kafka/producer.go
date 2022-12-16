package kafka

import (
	"go-kafka-tutorial/dtos"
	"log"
	"math/rand"
	"strconv"

	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde/avro"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type Producer struct{}

func (*Producer) Do(kafkaConfig kafka.ConfigMap, topic string, schemaRegistryUrl string) {
	client, err := schemaregistry.NewClient(schemaregistry.NewConfig(schemaRegistryUrl))
	if err != nil {
		log.Fatalf("Failed to initialize connection to schema registry, %v", err)
	}
	ser, err := avro.NewSpecificSerializer(client, serde.ValueSerde, avro.NewSerializerConfig())
	if err != nil {
		log.Fatalf("Failed to initialize value serializer, %v", err)
	}

	producer, err := kafka.NewProducer(&kafkaConfig)
	if err != nil {
		log.Fatal("Failed to register a producer", err.Error())
	}

	// Go-routine to handle message delivery reports and
	// possibly other event types (errors, stats, etc)
	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
				} else {
					log.Printf("Produced event to topic %s: key = %-10s value = %s\n",
						*ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))
				}
			}
		}
	}()

	users := []string{"eabara", "jsmith", "sgarcia", "jbernard", "htanaka", "awalther"}

	for n := 0; n < 100; n++ {
		user := dtos.User{
			User_id:      int32(n),
			User_name:    users[rand.Intn(len(users))],
			Phone_number: "1234567890",
		}
		payload, err := ser.Serialize(topic, &user)
		if err != nil {
			log.Printf("Failed to serialize: %v\n", user)
		}

		producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Key:   []byte(strconv.Itoa(n)),
			Value: payload,
		}, nil)
	}

	producer.Flush(15 * 1000)
	producer.Close()
}
