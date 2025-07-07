package main

import (
	"errors"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
)

func main() {

	topic := "test"

	conf := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	}

	producer, err := kafka.NewProducer(conf)
	if err != nil {
		panic(err)
	}

	for i := 0; i < 10000; i++ {
		id := uuid.New().String()
		err := producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Value: []byte(id),
			Key:   []byte(id),
		}, nil)

		if err == nil {
			continue
		}

		var kafkaError kafka.Error
		if errors.As(err, &kafkaError) && kafkaError.Code() == kafka.ErrQueueFull {
			time.Sleep(5 * time.Second)
		}
	}

	select {}
}
