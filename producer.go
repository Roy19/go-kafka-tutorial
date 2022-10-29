package main

import (
	"log"
	"math/rand"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func ProduceToQueue(kafkaConfig kafka.ConfigMap, topic string) {
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
	items := []string{"book", "alarm clock", "t-shirts", "gift card", "batteries"}

	for n := 0; n < 100; n++ {
		user := users[rand.Intn(len(users))]
		item := items[rand.Intn(len(items))]

		producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Key:   []byte(user),
			Value: []byte(item),
		}, nil)
	}

	producer.Flush(15 * 1000)
	producer.Close()
}
