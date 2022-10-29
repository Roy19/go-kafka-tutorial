package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func ConsumeFromQueue(kafkaConfig kafka.ConfigMap, topic string) {
	kafkaConfig["group.id"] = "go-kafka-tutorial"
	kafkaConfig["auto.offset.reset"] = "earliest"

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
			ev, err := consumer.ReadMessage(100 * time.Millisecond)
			if err != nil {
				log.Printf("Got error: %v\n", err)
				continue
			}
			log.Printf("Consumed event from topic %s: key = %-10s value = %s\n",
				*ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))
		}
	}

	consumer.Close()
}
