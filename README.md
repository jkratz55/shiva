# Shiva

<p align="center">
  <img src=".github/images/final-fantasy-shiva.png" alt="Final Fantasy - Shiva"/>
</p>

[![Go Reference](https://pkg.go.dev/badge/github.com/jkratz55/shiva.svg)](https://pkg.go.dev/github.com/jkratz55/shiva)
[![Go Report Card](https://goreportcard.com/badge/github.com/jkratz55/shiva)](https://goreportcard.com/report/github.com/jkratz55/shiva)
[![License](https://img.shields.io/github/license/jkratz55/shiva)](https://github.com/jkratz55/shiva/blob/master/LICENSE)
[![Release](https://img.shields.io/github/v/release/jkratz55/shiva)](https://github.com/jkratz55/shiva/releases)
[![Go Version](https://img.shields.io/github/go-mod/go-version/jkratz55/shiva)](https://go.dev/dl/)
[![Build Status](https://github.com/jkratz55/shiva/workflows/CI/badge.svg)](https://github.com/jkratz55/shiva/actions)
[![Coverage Status](https://coveralls.io/repos/github/jkratz55/shiva/badge.svg?branch=master)](https://coveralls.io/github/jkratz55/shiva?branch=master)

Shiva is a GO library/module for working with Kafka. Shiva provides friendly higher level APIs for consuming and
producing messages with Kafka. Under the hood Shiva uses the official Confluent Kafka GO
client (https://github.com/confluentinc/confluent-kafka-go). Some GO developers are very much opposed to using CGO, and
unfortunately, if you are dead set on avoiding CGO, this library may not be for you as it uses confluent-kafka-go, which
is a wrapper around librdkafka.

Shiva has a number of features that aim to make working with Kafka in GO easy:

* High-level and flexible Consumer API for consuming messages from Kafka
* High-level API for producing messages synchronously and asynchronously to Kafka.
* Support for OpenTelemetry tracing and metrics
* Built-in support for dead letter processing when a message cannot be processed
* Separates the concerns of Kafka from processing messages via the Handler interface

## Where Does the Name Shiva Come From?

Shiva is a frequently recurring Ice-elemental summon in the Final Fantasy series. Although enjoying regular appearances
throughout the series, Shiva, like most of the popular summonable entities, has not been given a significant back story,
being simply described as the "Ice Queen". As naming things can be quite hard, I've started naming my libraries and
packages based on video game lore and universes.

## Quickstart

Add shiva as a dependency

```shell
go get github.com/jkratz55/shiva
```

The following examples are demonstrating using OpenTelemetry for tracing and metrics (via Prometheus) along with
utilizing many of the hooks Shiva offers to invoke code on events. Depending on the use cases you may not need all the
features being shown in the examples below.

### Consumer

```go
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/sdk/metric"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/jkratz55/shiva"
	"github.com/jkratz55/shiva/shivaotel"
)

// ExampleHandler is a Handler implementation that just prints out the message
// key. In the real world you'd add your code/logic to process the message.
type ExampleHandler struct{}

func (e ExampleHandler) Handle(ctx context.Context, msg shiva.Message) error {
	// Simulate time it takes to process a message
	time.Sleep(time.Millisecond * 10)

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

```

### Producer

```go
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/sdk/metric"

	"github.com/jkratz55/shiva"
	"github.com/jkratz55/shiva/shivaotel"
)

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

	kafkaConfig := shiva.KafkaConfig{
		BootstrapServers: []string{"localhost:9092"},
		RequiredAcks:     shiva.AckLeader,
	}

	// Initial the ConsumerTelemetryProvider so we get metrics from the Consumer
	telemetryProvider, err := shivaotel.NewProducerTelemetryProvider(
		shivaotel.WithMeterProvider(meterProvider))
	if err != nil {
		panic(err)
	}

	producer, err := shiva.NewProducer(kafkaConfig,
		shiva.WithProducerTelemetryProvider(telemetryProvider))
	if err != nil {
		panic(err)
	}
	defer func() {
		producer.Flush(time.Second * 30)
		producer.Close()
	}()

	for i := 0; i < 100000; i++ {
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
```