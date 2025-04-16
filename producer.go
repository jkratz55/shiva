package shiva

import (
	"context"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type baseProducer interface {
	Produce(m *kafka.Message, deliveryChan chan kafka.Event) error
	Events() chan kafka.Event
	Flush(timeoutMs int) int
	Close()
	IsClosed() bool
	InitTransactions(ctx context.Context) error
	BeginTransaction() error
	AbortTransaction(ctx context.Context) error
	CommitTransaction(ctx context.Context) error
	Len() int
}

var _ baseProducer = &kafka.Producer{}

type Producer struct {
	base baseProducer
}

func NewProducer(base baseProducer) *Producer {
	return &Producer{}
}

func (p *Producer) Flush(timeout time.Duration) int {
	return p.base.Flush(int(timeout.Milliseconds()))
}

func (p *Producer) Close() {
	p.base.Close()
}
