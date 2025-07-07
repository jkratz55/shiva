package shiva

import (
	"context"
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Message represents a single message from Kafka.
type Message struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
	Timestamp time.Time
	Headers   []Header
	Opaque    interface{}
}

// Header represents a single Kafka message header.
//
// Message headers are made up of a list of Header elements, retaining their original insert order
// and allowing for duplicate Keys.
//
// Key is a human-readable string identifying the header. Value is the key's binary value, Kafka does
// not put any restrictions on the format of the Value, but it should be made relatively compact.
// The value may be a byte array, empty, or nil.
type Header struct {
	Key   string
	Value []byte
}

// mapMessage accepts a kafka.Message from the confluent-kafka SDK and maps it to Shiva's Message
// type.
func mapMessage(msg *kafka.Message) Message {
	m := Message{
		Topic:     *msg.TopicPartition.Topic,
		Partition: int(msg.TopicPartition.Partition),
		Offset:    int64(msg.TopicPartition.Offset),
		Key:       msg.Key,
		Value:     msg.Value,
		Timestamp: msg.Timestamp,
		Headers:   make([]Header, len(msg.Headers)),
	}
	for _, h := range msg.Headers {
		m.Headers = append(m.Headers, Header{
			Key:   h.Key,
			Value: h.Value,
		})
	}
	return m
}

// toKafkaMessage maps shiva representation of a kafka message to the confluent kafka go module's
// message type.
func toKafkaMessage(m Message) *kafka.Message {
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     StringPtr(m.Topic),
			Partition: int32(m.Partition),
			Offset:    kafka.Offset(m.Offset),
		},
		Value:     m.Value,
		Key:       m.Key,
		Timestamp: m.Timestamp,
		Opaque:    m.Opaque,
	}

	headers := make([]kafka.Header, 0)
	for _, header := range m.Headers {
		headers = append(headers, kafka.Header{
			Key:   header.Key,
			Value: header.Value,
		})
	}
	msg.Headers = headers

	return msg
}

// MessageBuilder represents a Kafka message and provides a fluent API for
// constructing a message to be produced to Kafka.
type MessageBuilder struct {
	producer *Producer
	topic    string
	key      string
	value    []byte
	headers  []Header
	opaque   interface{}
	err      error
}

// Topic sets the topic for the message.
func (m *MessageBuilder) Topic(t string) *MessageBuilder {
	m.topic = t
	return m
}

// Key sets the key for the message.
func (m *MessageBuilder) Key(k string) *MessageBuilder {
	m.key = k
	return m
}

// Value sets the value for the message.
//
// The behavior of Value varies based on the type of v:
//
//	[]byte - uses value as is
//	string - cast the value as []byte
//	encoding.BinaryMarshaler - invokes MarshalBinary and uses the resulting []byte
//
// If v is not any of the above types Value will attempt to marshal v as JSON.
func (m *MessageBuilder) Value(v interface{}) *MessageBuilder {
	switch val := v.(type) {
	case []byte:
		m.value = val
	case string:
		m.value = []byte(val)
	case encoding.BinaryMarshaler:
		data, err := val.MarshalBinary()
		m.err = err
		m.value = data
	default:
		data, err := json.Marshal(val)
		m.err = err
		m.value = data
	}
	return m
}

// JSON serializes the provided value to JSON and sets it as the value for the
// message.
func (m *MessageBuilder) JSON(v any) *MessageBuilder {
	data, err := json.Marshal(v)
	m.err = err
	m.value = data
	return m
}

// Header adds a header to the message.
func (m *MessageBuilder) Header(key string, value []byte) *MessageBuilder {
	m.headers = append(m.headers, Header{Key: key, Value: value})
	return m
}

// Opaque sets the opaque value for the message.
func (m *MessageBuilder) Opaque(o interface{}) *MessageBuilder {
	m.opaque = o
	return m
}

// ProduceAsync produces a message to Kafka asynchronously and returns immediately if
// the message was enqueued successfully, otherwise returns an error. The delivery
// report is delivered on the provided channel. If the channel is nil than Send
// operates as fire-and-forget.
func (m *MessageBuilder) ProduceAsync(ctx context.Context, deliveryCh chan DeliveryReport) error {
	if m.producer == nil {
		return fmt.Errorf("illegal state: producer is nil: MessageBuilder must be created by Producer using M() method")
	}

	if m.err != nil {
		return m.err
	}
	if strings.TrimSpace(m.topic) == "" {
		return errors.New("invalid message: no topic")
	}

	msg, err := m.Message()
	if err != nil {
		return fmt.Errorf("kafka: build message: %w", err)
	}

	return m.producer.ProduceAsync(ctx, msg, deliveryCh)
}

// Produce produces a message to Kafka and waits for the delivery report.
//
// This method is blocking and will wait until the delivery report is received
// from Kafka.
func (m *MessageBuilder) Produce(ctx context.Context) error {
	msg, err := m.Message()
	if err != nil {
		return fmt.Errorf("kafka: build message: %w", err)
	}
	return m.producer.Produce(ctx, msg)
}

// Message builds and returns a *kafka.Message instance from the Confluent Kafka
// library.
func (m *MessageBuilder) Message() (Message, error) {
	if m.err != nil {
		return Message{}, m.err
	}

	msg := Message{
		Topic:   m.topic,
		Key:     []byte(m.key),
		Value:   m.value,
		Headers: m.headers,
		Opaque:  m.opaque,
	}
	return msg, nil
}

// Err returns the last error that occurred while building the message or nil
// if there were no errors.
func (m *MessageBuilder) Err() error {
	return m.err
}
