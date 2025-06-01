package shivaotel

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"

	"github.com/jkratz55/shiva"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// ConsumerTelemetryProvider instruments and captures metrics for a Consumer using
// OpenTelemetry metrics.
//
// The zero-value is not usable. Initialize new instances using NewConsumerTelemetryProvider.
type ConsumerTelemetryProvider struct {
	// metrics
	messagesProcessed metric.Int64Counter
	kafkaErrors       metric.Int64Counter
	handlerErrors     metric.Int64Counter
	handlerLatency    metric.Float64Histogram
	rebalances        metric.Int64Counter
	lag               metric.Int64Gauge

	// tracing
	tracer trace.Tracer
}

// NewConsumerTelemetryProvider initializes a new ConsumerTelemetryProvider.
func NewConsumerTelemetryProvider(opts ...ConsumerOption) (*ConsumerTelemetryProvider, error) {

	baseOpts := make([]baseOption, len(opts))
	for i, opt := range opts {
		baseOpts[i] = opt
	}
	config := newConfig(baseOpts...)

	meter := config.meterProvider.Meter(Scope, metric.WithInstrumentationVersion(Version))

	messagesProcessed, err := meter.Int64Counter(
		"kafka.consumer.messages.processed",
		metric.WithDescription("Number of messages processed"))
	if err != nil {
		return nil, fmt.Errorf("initial metric kafka.consumer.messages.processed: %w", err)
	}

	kafkaErrors, err := meter.Int64Counter(
		"kafka.consumer.client.errors",
		metric.WithDescription("Number of errors the Kafka client encountered"))
	if err != nil {
		return nil, fmt.Errorf("initial metric kafka.consumer.client.errors: %w", err)
	}

	handlerErrors, err := meter.Int64Counter(
		"kafka.consumer.messages.failed",
		metric.WithDescription("Number of messages consumed that the application failed to process"))
	if err != nil {
		return nil, fmt.Errorf("initial metric kafka.consumer.messages.failed: %w", err)
	}

	handlerLatency, err := meter.Float64Histogram(
		"kafka.consumer.message.process.duration",
		metric.WithDescription("Duration for the Handler to process a message from Kafka"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(config.handlerHistogramBuckets...))
	if err != nil {
		return nil, fmt.Errorf("initial metric kafka.consumer.message.process.duration: %w", err)
	}

	rebalances, err := meter.Int64Counter(
		"kafka.consumer.rebalances",
		metric.WithDescription("Number of times the consumer has experience a rebalance event"))
	if err != nil {
		return nil, fmt.Errorf("initial metric kafka.consumer.rebalances: %w", err)
	}

	lag, err := meter.Int64Gauge(
		"kafka.consumer.lag",
		metric.WithDescription("Current lag for the topic/partitions assigned to the Consumer"))
	if err != nil {
		return nil, fmt.Errorf("initial metric kafka.consumer.lag: %w", err)
	}

	return &ConsumerTelemetryProvider{
		messagesProcessed: messagesProcessed,
		kafkaErrors:       kafkaErrors,
		handlerErrors:     handlerErrors,
		handlerLatency:    handlerLatency,
		rebalances:        rebalances,
		lag:               lag,
		tracer:            config.traceProvider.Tracer(Scope),
	}, nil
}

func (c *ConsumerTelemetryProvider) RecordMessageProcessed(handler string, topic string) {
	c.messagesProcessed.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String(labelHandler, handler),
		attribute.String(labelTopic, topic)))
}

func (c *ConsumerTelemetryProvider) RecordRebalance(handler string, groupId string) {
	c.rebalances.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String(labelHandler, handler),
		attribute.String(labelGroup, groupId)))
}

func (c *ConsumerTelemetryProvider) RecordHandlerError(handler string, topic string) {
	c.handlerErrors.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String(labelHandler, handler),
		attribute.String(labelTopic, topic)))
}

func (c *ConsumerTelemetryProvider) RecordKafkaError(handler string, topic string, code int) {
	c.kafkaErrors.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String(labelHandler, handler),
		attribute.String(labelTopic, topic),
		attribute.Int(labelCode, code)))
}

func (c *ConsumerTelemetryProvider) RecordHandlerExecutionDuration(handler string, topic string, dur time.Duration) {
	c.handlerLatency.Record(context.Background(), dur.Seconds(), metric.WithAttributes(
		attribute.String(labelHandler, handler),
		attribute.String(labelTopic, topic)))
}

func (c *ConsumerTelemetryProvider) RecordLag(handler string, groupId string, topic string, partition string, lag int64) {
	c.lag.Record(context.Background(), lag, metric.WithAttributes(
		attribute.String(labelHandler, handler),
		attribute.String(labelGroup, groupId),
		attribute.String(labelTopic, topic),
		attribute.String(labelPartition, partition)))
}

// Trace starts a new span/trace for the inbound Kafka message that is about to be processed. Trace
// returns a context.Context and a func. The context must be passed down to all future calls to
// properly trace the flow of processing the message. The func returned is a callback that must be
// invoked when the message has been processed (regardless if successful or not). The func returned
// records additional data on the span if err is non-nil, and ends the span.
func (c *ConsumerTelemetryProvider) Trace(msg shiva.Message) (context.Context, func(err error)) {
	carrier := propagation.HeaderCarrier{}
	for _, header := range msg.Headers {
		carrier.Set(header.Key, string(header.Value))
	}

	ctx := otel.GetTextMapPropagator().Extract(context.Background(), carrier)
	ctx, span := c.tracer.Start(ctx, "kafka.consumer.handler")

	span.SetAttributes(attribute.String("kafka.message.topic", msg.Topic),
		attribute.Int("kafka.message.partition", msg.Partition),
		attribute.Int64("kafka.message.offset", msg.Offset),
		attribute.String("kafka.message.key", string(msg.Key)))

	fn := func(err error) {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}

	return ctx, fn
}
