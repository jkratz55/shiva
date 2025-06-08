package shiva

import (
	"context"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type ConsumerTelemetryProvider interface {
	RecordMessageProcessed(handler string, topic string)
	RecordRebalance(handler string, groupId string)
	RecordHandlerError(handler string, topic string)
	RecordKafkaError(handler string, topic string, code int)
	RecordHandlerExecutionDuration(handler string, topic string, dur time.Duration)
	RecordLag(handler string, groupId string, topic string, partition string, lag int64)
	Trace(msg Message) (context.Context, func(err error))
}

type ProducerTelemetryProvider interface {
	RecordMessageDelivered(topic string)
	RecordDeliveryError(topic string)
	RecordKafkaError(code int)
	RecordDeliveryEnqueueError(topic string)
	Trace(ctx context.Context, msg *kafka.Message) (context.Context, func(err error))
	TraceDelivery(ctx context.Context) (context.Context, func(err error))
}

type NopConsumerTelemetryProvider struct {
}

func (n NopConsumerTelemetryProvider) RecordMessageProcessed(_ string, _ string) {}

func (n NopConsumerTelemetryProvider) RecordRebalance(_ string, _ string) {}

func (n NopConsumerTelemetryProvider) RecordHandlerError(_ string, _ string) {}

func (n NopConsumerTelemetryProvider) RecordKafkaError(_ string, _ string, _ int) {}

func (n NopConsumerTelemetryProvider) RecordHandlerExecutionDuration(_ string, _ string, _ time.Duration) {
}

func (n NopConsumerTelemetryProvider) RecordLag(_ string, _ string, _ string, _ string, _ int64) {}

func (n NopConsumerTelemetryProvider) Trace(_ Message) (context.Context, func(err error)) {
	return context.Background(), func(err error) {}
}

type NopProducerTelemetryProvider struct{}

func (n NopProducerTelemetryProvider) RecordMessageDelivered(topic string) {}

func (n NopProducerTelemetryProvider) RecordDeliveryError(topic string) {}

func (n NopProducerTelemetryProvider) RecordKafkaError(code int) {}

func (n NopProducerTelemetryProvider) RecordDeliveryEnqueueError(topic string) {}

func (n NopProducerTelemetryProvider) Trace(ctx context.Context, msg *kafka.Message) (context.Context, func(err error)) {
	return ctx, func(err error) {}
}

func (n NopProducerTelemetryProvider) TraceDelivery(ctx context.Context) (context.Context, func(err error)) {
	return ctx, func(err error) {}
}
