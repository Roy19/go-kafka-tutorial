package main

import (
	"go-kafka-tutorial/interfaces"
	"go-kafka-tutorial/kafka"
	"go-kafka-tutorial/utils"
	"log"
	"os"
)

const (
	produce = "produce"
	consume = "consume"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatalf("Usage: kafka-tutorial command\n\n" +
			"Commands:\n" +
			"produce    produce events to kafka queue\n" +
			"consume    consume events from kafka queue\n")
	}

	if os.Args[1] != produce && os.Args[1] != consume {
		log.Fatalf("Wrong command")
	}

	conf := utils.ReadConfig()

	topic := "go-kafka-topic"

	tasks := map[string]interfaces.IAction{
		produce: &kafka.Producer{},
		consume: &kafka.Consumer{},
	}

	tasks[os.Args[1]].Do(conf, topic)
}
