package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/sdk/metric"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/jkratz55/shiva"
	"github.com/jkratz55/shiva/shivaotel"
)

// ExampleHandler is a Handler implementation that just prints out the message
// key. In the real world you'd add your code/logic to process the message.
type ExampleHandler struct{}

func (e ExampleHandler) Handle(msg shiva.Message) error {
	fmt.Println(msg.Key)
	return nil
}

// DeadLetterHandler is a simple and silly example that simply logs the failed
// message. In some circumstances this may be fine if you have no need to
// re-process the message, but generally you will probably want to save the
// message to a database, disk, or publish it to a retry or dead letter topic
// so it can be retried.
type DeadLetterHandler struct {
	logger *slog.Logger
}

func NewDeadLetterHandler(l *slog.Logger) *DeadLetterHandler {
	return &DeadLetterHandler{
		logger: l,
	}
}

func (d DeadLetterHandler) Handle(msg shiva.Message, err error) {
	d.logger.Error("Something went wrong",
		slog.String("err", err.Error()),
		slog.Group("kafka",
			slog.Any("msg", msg)))
}

// ConsumerHooks is a type that holds a reference to a logger and has methods
// conforming to all the hooks we care about from the Consumer. We could, of course,
// have had standalone functions, but this makes it cleaner and potentially more
// re-usable.
type ConsumerHooks struct {
	logger *slog.Logger
}

func NewConsumerHooks(l *slog.Logger) *ConsumerHooks {
	return &ConsumerHooks{
		logger: l,
	}
}

func (ch *ConsumerHooks) OnErr(err error) {
	ch.logger.Error("Kafka Consumer Error",
		slog.String("err", err.Error()))
}

func (ch *ConsumerHooks) OnOffsetsCommitted(offsets shiva.TopicPartitions, err error) {
	if err != nil {
		ch.logger.Error("Failed to commit offsets for one or more partitions",
			slog.String("err", err.Error()))
	}

	for _, offset := range offsets {
		ch.logger.Info("Offsets committed to Kafka",
			slog.Group("kafka",
				slog.String("topic", offset.Topic),
				slog.Int("partition", offset.Partition),
				slog.Int64("offset", offset.Offset)))
	}
}

func (ch *ConsumerHooks) OnAssigned(partitions shiva.TopicPartitions) {
	ch.logger.Info("A rebalance event occurred for consumer group")
	for _, partition := range partitions {
		ch.logger.Info("Consumer was assigned a topic/partition",
			slog.Group("kafka",
				slog.String("topic", partition.Topic),
				slog.Int("partition", partition.Partition)))
	}
}

func (ch *ConsumerHooks) OnRevoked(partitions shiva.TopicPartitions) {
	ch.logger.Info("A rebalance event occurred for consumer group")
	for _, partition := range partitions {
		ch.logger.Info("Brokers revoked assigned for topic/partition",
			slog.Group("kafka",
				slog.String("topic", partition.Topic),
				slog.Int("partition", partition.Partition)))
	}
}

func main() {

	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelDebug,
	}))

	// Setup exporter to expose Consumer metrics to Prometheus
	promExporter, err := prometheus.New()
	if err != nil {
		panic(err)
	}
	defer promExporter.Shutdown(context.Background())

	meterProvider := metric.NewMeterProvider(metric.WithReader(promExporter))
	defer meterProvider.Shutdown(context.Background())

	// Setup Kafka configuration
	kafkaConfig := shiva.KafkaConfig{
		BootstrapServers:       []string{"localhost:9092"},
		GroupID:                "shiva-test",
		AutoOffsetReset:        shiva.Earliest,
		AcknowledgmentStrategy: shiva.AcknowledgmentStrategyPostProcessing,
	}

	var handler shiva.Handler
	handler = &ExampleHandler{}
	hooks := NewConsumerHooks(logger)
	dlHandler := NewDeadLetterHandler(logger)

	// Callback that is invoked whenever the Retry middleware encounters an error
	retryOnErr := func(err error) {
		fmt.Println(err)
	}

	// Initial the ConsumerTelemetryProvider so we get metrics from the Consumer
	telemetryProvider, err := shivaotel.NewConsumerTelemetryProvider(
		shivaotel.WithMeterProvider(meterProvider))
	if err != nil {
		panic(err)
	}

	// Wrap the ExampleHandler with Retry middleware and only retry if errors are
	// marked as retryable
	handler = shiva.Retry(handler,
		shiva.WithMaxAttempts(5),
		shiva.WithOnError(retryOnErr),
		shiva.WithRetryableErrorsOnly(true))(handler)

	// Initialize the Consumer with options for dead letter processing, and hooks/callbacks
	consumer, err := shiva.NewConsumer(kafkaConfig, "test", handler,
		shiva.WithOnOffsetsCommitted(hooks.OnOffsetsCommitted),
		shiva.WithOnErr(hooks.OnErr),
		shiva.WithOnAssigned(hooks.OnAssigned),
		shiva.WithOnRevoked(hooks.OnRevoked),
		shiva.WithDeadLetterHandler(dlHandler),
		shiva.WithConsumerTelemetryProvider(telemetryProvider),
		shiva.WithName("test-consumer"))
	if err != nil {
		panic(err)
	}

	// Start an http server for Prometheus to scrape
	promServer := http.Server{
		Addr:    ":8082",
		Handler: promhttp.Handler(),
	}
	go func() {
		// Don't ignore the error for real code
		_ = promServer.ListenAndServe()
	}()

	// Run the consumer
	go func() {
		err := consumer.Run()
		if err != nil {
			panic(err)
		}
	}()

	// Block forever, don't do this in real code
	select {}
}
