package interfaces

import "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"

type IAction interface {
	Do(kafka.ConfigMap, string, string)
}
