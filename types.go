package shiva

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Stats represents statistics from Confluent Kafka / librdkafka client.
type Stats = kafka.Stats

// Watermark is a data structure representing the lowest and highest offsets for
// a topic/partition.
type Watermark struct {
	Low  int64
	High int64
}

// TopicPartition represents a topic and partition along with its offset.
type TopicPartition struct {
	Topic     string
	Partition int
	Offset    int64
}

// TopicPartitions represents an array of TopicPartition
type TopicPartitions []TopicPartition

func mapTopicPartition(tp *kafka.TopicPartition) TopicPartition {
	return TopicPartition{
		Topic:     *tp.Topic,
		Partition: int(tp.Partition),
		Offset:    int64(tp.Offset),
	}
}

func mapTopicPartitions(tps kafka.TopicPartitions) TopicPartitions {
	topicPartitions := make(TopicPartitions, 0, len(tps))
	for _, tp := range tps {
		topicPartitions = append(topicPartitions, mapTopicPartition(&tp))
	}
	return topicPartitions
}
