package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	"github.com/jkratz55/shiva"
	"github.com/jkratz55/shiva/shivaotel"
)

func main() {

	// --------------------------------------------------------------------------------------------
	// Setup logging and telemetry
	// --------------------------------------------------------------------------------------------

	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelDebug,
	}))

	// Setup OpenTelemetry trace exporter
	traceExporter, err := otlptracehttp.New(context.Background())
	if err != nil {
		logger.Error("error creating trace exporter", slog.String("err", err.Error()))
		panic(err)
	}

	// Configure OpenTelemetry resource and TracerProvider
	otelResource := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceNameKey.String("cacheotel-example"),
		semconv.ServiceVersionKey.String("1.0.0"))
	traceProvider := trace.NewTracerProvider(
		trace.WithBatcher(traceExporter),
		trace.WithResource(otelResource))
	defer func() {
		err := traceProvider.Shutdown(context.Background())
		if err != nil {
			logger.Error("error shutting down trace provider", slog.String("err", err.Error()))
		}
	}()

	// Set the TraceProvider and TextMapPropagator globally
	otel.SetTracerProvider(traceProvider)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	// Setup OpenTelemetry metric exporter
	exporter, err := prometheus.New()
	if err != nil {
		logger.Error("error creating prometheus exporter", slog.String("err", err.Error()))
		panic(err)
	}
	provider := metric.NewMeterProvider(metric.WithReader(exporter))
	otel.SetMeterProvider(provider)
	defer provider.Shutdown(context.Background())

	kafkaConfig := shiva.KafkaConfig{
		BootstrapServers:       []string{"localhost:9092"},
		RequiredAcks:           shiva.AckLeader,                            // only applies to producer
		GroupID:                "shiva-test",                               // only applies to consumer
		AutoOffsetReset:        shiva.Earliest,                             // only applies to consumer
		AcknowledgmentStrategy: shiva.AcknowledgmentStrategyPostProcessing, // only applies to consumer
	}

	// Initial the ConsumerTelemetryProvider so we get metrics from the Consumer
	telemetryProvider, err := shivaotel.NewProducerTelemetryProvider(
		shivaotel.WithMeterProvider(provider))
	if err != nil {
		panic(err)
	}

	// --------------------------------------------------------------------------------------------
	// Setup producer and produce sample messages
	// --------------------------------------------------------------------------------------------

	producer, err := shiva.NewProducer(kafkaConfig,
		shiva.WithProducerTelemetryProvider(telemetryProvider))
	if err != nil {
		panic(err)
	}
	defer func() {
		producer.Flush(time.Second * 30)
		producer.Close()
	}()

	for i := 0; i < 10; i++ {
		err := producer.M().
			Topic("test").
			Key(uuid.New().String()).
			Value("Hello World!").
			Produce(context.Background())
		if err != nil {
			logger.Error("Failed to produce message",
				slog.String("err", err.Error()))
		}
	}

	time.Sleep(time.Second * 10)

	// --------------------------------------------------------------------------------------------
	// Consume messages with telemetry enabled so we can see the trace from producer to consumer
	// --------------------------------------------------------------------------------------------

	var handler shiva.Handler
	handler = &ExampleHandler{}
	hooks := NewConsumerHooks(logger)
	dlHandler := NewDeadLetterHandler(logger)

	// Callback that is invoked whenever the Retry middleware encounters an error
	retryOnErr := func(err error) {
		fmt.Println(err)
	}

	// Initial the ConsumerTelemetryProvider so we get metrics from the Consumer
	consumerTelemetryProvider, err := shivaotel.NewConsumerTelemetryProvider(
		shivaotel.WithMeterProvider(provider))
	if err != nil {
		panic(err)
	}

	// Wrap the ExampleHandler with Retry middleware and only retry if errors are
	// marked as retryable
	handler = shiva.Retry(handler,
		shiva.WithMaxAttempts(5),
		shiva.WithOnError(retryOnErr),
		shiva.WithRetryableErrorsOnly(true))

	// Initialize the Consumer with options for dead letter processing, and hooks/callbacks
	consumer, err := shiva.NewConsumer(kafkaConfig, "test", handler,
		shiva.WithOnOffsetsCommitted(hooks.OnOffsetsCommitted),
		shiva.WithOnErr(hooks.OnErr),
		shiva.WithOnAssigned(hooks.OnAssigned),
		shiva.WithOnRevoked(hooks.OnRevoked),
		shiva.WithDeadLetterHandler(dlHandler),
		shiva.WithConsumerTelemetryProvider(consumerTelemetryProvider),
		shiva.WithName("test-consumer"))
	if err != nil {
		panic(err)
	}

	// Run the consumer
	go func() {
		err := consumer.Run()
		if err != nil {
			panic(err)
		}
	}()

	// --------------------------------------------------------------------------------------------
	// Setup http server for Prometheus metrics
	// --------------------------------------------------------------------------------------------

	// Start an http server for Prometheus to scrape
	promServer := http.Server{
		Addr:    ":8082",
		Handler: promhttp.Handler(),
	}
	go func() {
		// Don't ignore the error for real code
		_ = promServer.ListenAndServe()
	}()

	// Block forever, don't do this in real code
	select {}
}

// --------------------------------------------------------------------------------------------
// Handling example code for the
// --------------------------------------------------------------------------------------------

// ExampleHandler is a Handler implementation that just prints out the message
// key. In the real world you'd add your code/logic to process the message.
type ExampleHandler struct{}

func (e ExampleHandler) Handle(ctx context.Context, msg shiva.Message) error {
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

func (d DeadLetterHandler) Handle(ctx context.Context, msg shiva.Message, err error) {
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
