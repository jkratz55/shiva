package shiva

import (
	"time"
)

type ConsumerTelemetryProvider interface {
	RecordMessageProcessed(handler string, topic string)
	RecordRebalance(handler string, groupId string)
	RecordHandlerError(handler string, topic string)
	RecordKafkaError(handler string, topic string, code int)
	RecordHandlerExecutionDuration(handler string, topic string, dur time.Duration)
	RecordLag(handler string, groupId string, topic string, partition string, lag int64)
}

type ProducerTelemetryProvider interface {
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

type NopProducerTelemetryProvider struct {
}
