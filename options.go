package shiva

import (
	"time"
)

type options struct {

	// common options
	logger Logger
	onErr  func(err error)

	// consumer options
	deadLetterHandler  DeadLetterHandler
	onAssigned         func(tps TopicPartitions)
	onRevoked          func(tps TopicPartitions)
	onStats            func(data map[string]any)
	statsEnabled       bool
	statsInterval      int
	onOffsetsCommitted func(offsets TopicPartitions, err error)
}

func newOptions(opts ...baseOption) *options {
	o := &options{
		logger:            NewNopLogger(),
		onErr:             func(err error) {},
		deadLetterHandler: DeadLetterHandlerFunc(func(msg Message, err error) {}),
	}

	for _, opt := range opts {
		opt.apply(o)
	}

	return o
}

type baseOption interface {
	apply(opts *options)
}

type Option interface {
	baseOption
	consumer()
}

type option func(opt *options)

func (fn option) consumer() {}

func (fn option) apply(opt *options) {
	fn(opt)
}

type ConsumerOption interface {
	baseOption
	consumer()
}

type consumerOption func(opt *options)

var _ ConsumerOption = (*consumerOption)(nil)

func (c consumerOption) apply(opts *options) {
	c(opts)
}

func (c consumerOption) consumer() {}

// WithLogger sets a Logger to be used for a component.
func WithLogger(logger Logger) Option {
	return option(func(opt *options) {
		if logger == nil {
			logger = NewNopLogger()
		}
		opt.logger = logger
	})
}

// WithOnErr sets a callback invoked on an error for a component.
func WithOnErr(fn func(err error)) Option {
	if fn == nil {
		fn = func(err error) {}
	}
	return option(func(opt *options) {
		opt.onErr = fn
	})
}

// WithDeadLetterHandler sets a DeadLetterHandler that is invoked by the Consumer
// when the Handler returns an error signaling it failed to process the message.
func WithDeadLetterHandler(dlh DeadLetterHandler) ConsumerOption {
	if dlh == nil {
		dlh = DeadLetterHandlerFunc(func(msg Message, err error) {})
	}
	return consumerOption(func(opt *options) {
		opt.deadLetterHandler = dlh
	})
}

// WithOnAssigned sets a callback that will be invoked by the Consumer when a
// rebalance occurs and topics/partitions are assigned.
func WithOnAssigned(fn func(tps TopicPartitions)) ConsumerOption {
	if fn == nil {
		fn = func(tps TopicPartitions) {}
	}
	return consumerOption(func(opt *options) {
		opt.onAssigned = fn
	})
}

// WithOnRevoked sets a callback that will be invoked by the Consumer when a
// rebalance occurs and topics/partitions are revoked.
func WithOnRevoked(fn func(tps TopicPartitions)) ConsumerOption {
	if fn == nil {
		fn = func(tps TopicPartitions) {}
	}
	return consumerOption(func(opt *options) {
		opt.onRevoked = fn
	})
}

// WithStats enables statistics for the underlying Confluent Kafka / librdkafka
// client at the provided interval. On the provided interval stats will be
// fetched, and fn will be invoked with the results.
func WithStats(interval time.Duration, fn func(stats map[string]any)) ConsumerOption {
	if fn == nil {
		fn = func(stats map[string]any) {}
	}
	return consumerOption(func(opt *options) {
		opt.onStats = fn
		opt.statsEnabled = true
		opt.statsInterval = int(interval.Milliseconds())
	})
}

// WithOnOffsetsCommitted sets a callback that will be invoked by the Consumer when
// offsets are committed back to the Kafka brokers.
func WithOnOffsetsCommitted(fn func(offsets TopicPartitions, err error)) ConsumerOption {
	if fn == nil {
		fn = func(offsets TopicPartitions, err error) {}
	}
	return consumerOption(func(opt *options) {
		opt.onOffsetsCommitted = fn
	})
}
