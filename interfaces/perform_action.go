package interfaces

import "github.com/confluentinc/confluent-kafka-go/kafka"

type IAction interface {
	Do(kafka.ConfigMap, string)
}
