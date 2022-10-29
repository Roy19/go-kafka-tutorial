package main

import (
	"fmt"
	"log"
	"os"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Printf("Usage: kafka-tutorial <config file> command\n\n" +
			"Commands:\n" +
			"produce    produce events to kafka queue\n" +
			"consume    consume events from kafka queue\n")
		os.Exit(1)
	}

	configFile := os.Args[1]
	conf := ReadConfig(configFile)

	topic := "go-kafka-topic"

	switch {
	case os.Args[2] == "produce":
		ProduceToQueue(conf, topic)
	case os.Args[2] == "consume":
		ConsumeFromQueue(conf, topic)
	default:
		log.Fatal("Wrong command")
	}
}
