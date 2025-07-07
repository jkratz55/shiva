package shiva

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/mock"
)

type mockHandler struct {
	mock.Mock

	attempts int
}

func (m *mockHandler) Handle(ctx context.Context, msg Message) error {
	m.attempts++
	args := m.Called(ctx, msg)
	if args.Get(0) != nil {
		return args.Error(0)
	}
	return nil
}

type mockDeadLetterHandler struct {
	mock.Mock

	called int
}

func (m *mockDeadLetterHandler) Handle(ctx context.Context, msg Message, err error) {
	m.called++
	_ = m.Called(ctx, msg, err)
}

type mockKafkaConsumer struct {
	mock.Mock
}

func (m *mockKafkaConsumer) Assignment() ([]kafka.TopicPartition, error) {
	args := m.Called()
	if args.Get(1) != nil {
		return args.Get(0).([]kafka.TopicPartition), args.Error(1)
	}
	return args.Get(0).([]kafka.TopicPartition), nil
}

func (m *mockKafkaConsumer) Subscription() (topics []string, err error) {
	args := m.Called()
	if args.Get(1) != nil {
		return args.Get(0).([]string), args.Error(1)
	}
	return args.Get(0).([]string), nil
}

func (m *mockKafkaConsumer) Committed(partitions []kafka.TopicPartition, timeoutMs int) (offsets []kafka.TopicPartition, err error) {
	args := m.Called(partitions, timeoutMs)
	if args.Get(1) != nil {
		return args.Get(0).([]kafka.TopicPartition), args.Error(1)
	}
	return args.Get(0).([]kafka.TopicPartition), nil
}

func (m *mockKafkaConsumer) QueryWatermarkOffsets(topic string, partition int32, timeoutMs int) (low, high int64, err error) {
	args := m.Called(topic, partition, timeoutMs)
	if args.Get(2) != nil {
		return args.Get(0).(int64), args.Get(1).(int64), args.Error(2)
	}
	return args.Get(0).(int64), args.Get(1).(int64), nil
}

func (m *mockKafkaConsumer) GetWatermarkOffsets(topic string, partition int32) (low, high int64, err error) {
	args := m.Called(topic, partition)
	if args.Get(2) != nil {
		return args.Get(0).(int64), args.Get(1).(int64), args.Error(2)
	}
	return args.Get(0).(int64), args.Get(1).(int64), nil
}

func (m *mockKafkaConsumer) Subscribe(topics string, rebalanceCb kafka.RebalanceCb) error {
	args := m.Called(topics, rebalanceCb)
	if args.Get(0) != nil {
		return args.Error(0)
	}
	return nil
}

func (m *mockKafkaConsumer) Poll(timeoutMs int) (event kafka.Event) {
	args := m.Called(timeoutMs)
	return args.Get(0).(kafka.Event)
}

func (m *mockKafkaConsumer) CommitMessage(msg *kafka.Message) ([]kafka.TopicPartition, error) {
	args := m.Called(msg)
	if args.Get(1) != nil {
		return args.Get(0).([]kafka.TopicPartition), args.Error(1)
	}
	return args.Get(0).([]kafka.TopicPartition), nil
}

func (m *mockKafkaConsumer) StoreMessage(msg *kafka.Message) (storedOffsets []kafka.TopicPartition, err error) {
	args := m.Called(msg)
	if args.Get(1) != nil {
		return args.Get(0).([]kafka.TopicPartition), args.Error(1)
	}
	return args.Get(0).([]kafka.TopicPartition), nil
}

func (m *mockKafkaConsumer) Commit() ([]kafka.TopicPartition, error) {
	args := m.Called()
	if args.Get(1) != nil {
		return args.Get(0).([]kafka.TopicPartition), args.Error(1)
	}
	return args.Get(0).([]kafka.TopicPartition), nil
}

func (m *mockKafkaConsumer) Position(partitions []kafka.TopicPartition) (offsets []kafka.TopicPartition, err error) {
	args := m.Called(partitions)
	if args.Get(1) != nil {
		return args.Get(0).([]kafka.TopicPartition), args.Error(1)
	}
	return args.Get(0).([]kafka.TopicPartition), nil
}

func (m *mockKafkaConsumer) Pause(partitions []kafka.TopicPartition) (err error) {
	args := m.Called(partitions)
	if args.Get(0) != nil {
		return args.Error(0)
	}
	return nil
}

func (m *mockKafkaConsumer) Resume(partitions []kafka.TopicPartition) (err error) {
	args := m.Called(partitions)
	if args.Get(0) != nil {
		return args.Error(0)
	}
	return nil
}

func (m *mockKafkaConsumer) IsClosed() bool {
	args := m.Called()
	return args.Bool(0)
}

func (m *mockKafkaConsumer) Close() error {
	args := m.Called()
	if args.Get(0) != nil {
		return args.Error(0)
	}
	return nil
}

type mockKafkaProducer struct {
	mock.Mock
}

func (mp *mockKafkaProducer) Produce(m *kafka.Message, deliveryChan chan kafka.Event) error {
	args := mp.Called(m, deliveryChan)
	if args.Get(0) != nil {
		return args.Error(0)
	}
	return nil
}

func (mp *mockKafkaProducer) Events() chan kafka.Event {
	args := mp.Called()
	return args.Get(0).(chan kafka.Event)
}

func (mp *mockKafkaProducer) Flush(timeoutMs int) int {
	args := mp.Called(timeoutMs)
	return args.Int(0)
}

func (mp *mockKafkaProducer) Close() {
	_ = mp.Called()
}

func (mp *mockKafkaProducer) IsClosed() bool {
	args := mp.Called()
	return args.Bool(0)
}

func (mp *mockKafkaProducer) InitTransactions(ctx context.Context) error {
	args := mp.Called(ctx)
	if args.Get(0) != nil {
		return args.Error(0)
	}
	return nil
}

func (mp *mockKafkaProducer) BeginTransaction() error {
	args := mp.Called()
	if args.Get(0) != nil {
		return args.Error(0)
	}
	return nil
}

func (mp *mockKafkaProducer) AbortTransaction(ctx context.Context) error {
	args := mp.Called(ctx)
	if args.Get(0) != nil {
		return args.Error(0)
	}
	return nil
}

func (mp *mockKafkaProducer) CommitTransaction(ctx context.Context) error {
	args := mp.Called(ctx)
	if args.Get(0) != nil {
		return args.Error(0)
	}
	return nil
}

func (mp *mockKafkaProducer) Len() int {
	args := mp.Called()
	return args.Int(0)
}

func (mp *mockKafkaProducer) Purge(flags int) error {
	args := mp.Called(flags)
	if args.Get(0) != nil {
		return args.Error(0)
	}
	return nil
}
