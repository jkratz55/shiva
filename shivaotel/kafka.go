package shivaotel

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type KafkaHeaderCarrier []kafka.Header

func (k *KafkaHeaderCarrier) Get(key string) string {
	for _, header := range *k {
		if header.Key == key {
			return string(header.Value)
		}
	}
	return ""
}

func (k *KafkaHeaderCarrier) Set(key string, value string) {
	for i, header := range *k {
		if header.Key == key {
			(*k)[i].Value = []byte(value)
			return
		}
	}
	*k = append(*k, kafka.Header{
		Key:   key,
		Value: []byte(value),
	})
}

func (k *KafkaHeaderCarrier) Keys() []string {
	keys := make([]string, 0)
	for _, header := range *k {
		keys = append(keys, header.Key)
	}
	return keys
}
