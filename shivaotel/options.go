package shivaotel

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

type config struct {
	meterProvider metric.MeterProvider

	handlerHistogramBuckets []float64
}

func newConfig(opts ...baseOption) *config {
	conf := &config{
		meterProvider:           otel.GetMeterProvider(),
		handlerHistogramBuckets: []float64{0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.000},
	}

	for _, opt := range opts {
		opt.apply(conf)
	}

	return conf
}

type baseOption interface {
	apply(opts *config)
}

type Option interface {
	baseOption
	consumer()
}

type option func(opt *config)

func (fn option) consumer() {}

func (fn option) apply(opt *config) {
	fn(opt)
}

type ConsumerOption interface {
	baseOption
	consumer()
}

type consumerOption func(opt *config)

var _ ConsumerOption = (*consumerOption)(nil)

func (c consumerOption) apply(opts *config) {
	c(opts)
}

func (c consumerOption) consumer() {}

// WithMeterProvider allows a custom-configured MeterProvider to be used for
// instrumenting with OpenTelemetry.
func WithMeterProvider(mp metric.MeterProvider) Option {
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	return option(func(opt *config) {
		opt.meterProvider = mp
	})
}

// WithHandlerHistogramBuckets sets the buckets for the Consumer Handler execution
// duration.
func WithHandlerHistogramBuckets(buckets ...float64) ConsumerOption {
	return option(func(opt *config) {
		opt.handlerHistogramBuckets = buckets
	})
}
