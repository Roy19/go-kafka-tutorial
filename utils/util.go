package utils

import (
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func ReadConfig() kafka.ConfigMap {
	return kafka.ConfigMap{
		"bootstrap.servers": os.Getenv("KAFKA_EVENTHUB_ENDPOINT"),
		"sasl.mechanisms":   "PLAIN",
		"security.protocol": "SASL_SSL",
		"sasl.username":     "$ConnectionString",
		"sasl.password":     os.Getenv("KAFKA_EVENTHUB_CONNECTION_STRING"),
	}
}