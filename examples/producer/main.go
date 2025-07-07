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
