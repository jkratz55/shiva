package shivaotel

import (
	"context"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type ProducerTelemetryProvider struct {
	// metrics
	messagesDelivered       metric.Int64Counter
	deliveryFailures        metric.Int64Counter
	kafkaErrors             metric.Int64Counter
	deliveryEnqueueFailures metric.Int64Counter

	// tracing
	tracer trace.Tracer
}

func NewProducerTelemetryProvider(opts ...ProducerOption) (*ProducerTelemetryProvider, error) {
	baseOpts := make([]baseOption, len(opts))
	for i, opt := range opts {
		baseOpts[i] = opt
	}
	config := newConfig(baseOpts...)

	meter := config.meterProvider.Meter(Scope, metric.WithInstrumentationVersion(Version))

	messagesDelivered, err := meter.Int64Counter(
		"kafka.producer.messages.delivered",
		metric.WithDescription("Number of messages delivered"))
	if err != nil {
		return nil, fmt.Errorf("initial metric kafka.producer.messages.delivered: %w", err)
	}

	deliveryFailures, err := meter.Int64Counter(
		"kafka.producer.messages.delivery.failures",
		metric.WithDescription("Number of messages that failed to be delivered"))
	if err != nil {
		return nil, fmt.Errorf("initial metric kafka.producer.messages.delivery.failures: %w", err)
	}

	kafkaErrors, err := meter.Int64Counter(
		"kafka.producer.client.errors",
		metric.WithDescription("Number of errors the Kafka client encountered"))
	if err != nil {
		return nil, fmt.Errorf("initial metric kafka.producer.client.errors: %w", err)
	}

	deliveryEnqueueFailures, err := meter.Int64Counter(
		"kafka.producer.messages.delivery.enqueue.failures",
		metric.WithDescription("Number of messages that failed to be enqueued"))
	if err != nil {
		return nil, fmt.Errorf("initial metric kafka.producer.messages.delivery.enqueue.failures: %w", err)
	}

	return &ProducerTelemetryProvider{
		messagesDelivered:       messagesDelivered,
		deliveryFailures:        deliveryFailures,
		kafkaErrors:             kafkaErrors,
		deliveryEnqueueFailures: deliveryEnqueueFailures,
		tracer:                  config.traceProvider.Tracer(Scope),
	}, nil
}

func (p *ProducerTelemetryProvider) RecordMessageDelivered(topic string) {
	p.messagesDelivered.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String(labelTopic, topic)))
}

func (p *ProducerTelemetryProvider) RecordDeliveryError(topic string) {
	p.deliveryFailures.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String(labelTopic, topic)))
}

func (p *ProducerTelemetryProvider) RecordKafkaError(code int) {
	p.kafkaErrors.Add(context.Background(), 1, metric.WithAttributes(
		attribute.Int(labelCode, code)))
}

func (p *ProducerTelemetryProvider) RecordDeliveryEnqueueError(topic string) {
	p.deliveryEnqueueFailures.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String(labelTopic, topic)))
}

func (p *ProducerTelemetryProvider) Trace(ctx context.Context, msg *kafka.Message) (context.Context, func(err error)) {

	ctx, span := p.tracer.Start(ctx, "kafka.producer")
	span.SetAttributes(attribute.String("kafka.message.topic", *msg.TopicPartition.Topic),
		attribute.Int("kafka.message.partition", int(msg.TopicPartition.Partition)),
		attribute.String("kafka.message.key", string(msg.Key)))

	headers := KafkaHeaderCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, &headers)

	msg.Headers = append(msg.Headers, headers...)

	fn := func(err error) {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}

	return ctx, fn
}

func (p *ProducerTelemetryProvider) TraceDelivery(ctx context.Context) (context.Context, func(err error)) {

	ctx, span := p.tracer.Start(ctx, "kafka.producer.delivery")

	fn := func(err error) {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}

	return ctx, fn
}
