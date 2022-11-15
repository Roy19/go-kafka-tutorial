package main

import (
	"go-kafka-tutorial/interfaces"
	"go-kafka-tutorial/kafka"
	"log"
	"os"
)

const (
	produce = "produce"
	consume = "consume"
)

func main() {
	if len(os.Args) != 3 {
		log.Fatalf("Usage: kafka-tutorial <config file> command\n\n" +
			"Commands:\n" +
			"produce    produce events to kafka queue\n" +
			"consume    consume events from kafka queue\n")
	}

	if os.Args[2] != produce && os.Args[2] != consume {
		log.Fatalf("Wrong command")
	}

	configFile := os.Args[1]
	conf := ReadConfig(configFile)

	topic := "go-kafka-topic"

	tasks := map[string]interfaces.IAction{
		produce: &kafka.Producer{},
		consume: &kafka.Consumer{},
	}

	tasks[os.Args[2]].Do(conf, topic)
}
