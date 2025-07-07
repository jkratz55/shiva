package shiva

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type kafkaProducer interface {
	Produce(m *kafka.Message, deliveryChan chan kafka.Event) error
	Events() chan kafka.Event
	Flush(timeoutMs int) int
	Close()
	IsClosed() bool
	InitTransactions(ctx context.Context) error
	BeginTransaction() error
	AbortTransaction(ctx context.Context) error
	CommitTransaction(ctx context.Context) error
	Len() int
	Purge(flags int) error
}

var _ kafkaProducer = &kafka.Producer{}

type Producer struct {
	base         kafkaProducer
	loggerStopCh chan struct{}
	eventStopCh  chan struct{}
	logChan      chan kafka.LogEvent

	// callbacks
	onErr   func(err error)
	onStats func(stats map[string]any)

	// telemetry
	telemetryProvider ProducerTelemetryProvider
}

func NewProducer(conf KafkaConfig, opts ...ProducerOption) (*Producer, error) {

	// Initialize the defaults for the KafkaConfig instance
	conf.init()

	// Apply any options provided
	baseOpts := make([]baseOption, len(opts))
	for i, opt := range opts {
		baseOpts[i] = opt
	}
	options := newOptions(baseOpts...)

	configMap := producerConfigMap(conf)

	loggerStopCh := make(chan struct{})
	eventStopCh := make(chan struct{})
	logChan := make(chan kafka.LogEvent, 1000)

	// Start goroutine to read logs from librdkafka and uses Logger to log
	// them rather than dumping them to stdout
	go func(logger Logger) {
		for {
			select {
			case logEvent, ok := <-logChan:
				if !ok {
					return
				}
				logger.Debug(logEvent.Message,
					"source", "librdkafka",
					"name", logEvent.Name,
					"tag", logEvent.Tag,
					"level", logEvent.Level)
			case <-loggerStopCh:
				return
			}
		}
	}(options.logger)

	// Configure logs from librdkafka to be sent to our logger rather than stdout
	_ = configMap.SetKey("go.logs.channel.enable", true)
	_ = configMap.SetKey("go.logs.channel", logChan)

	base, err := kafka.NewProducer(configMap)
	if err != nil {
		return nil, fmt.Errorf("kafka: failed to initialize Confluent Kafka Producer: %w", err)
	}

	producer := &Producer{
		base:              base,
		loggerStopCh:      loggerStopCh,
		eventStopCh:       eventStopCh,
		logChan:           logChan,
		onErr:             options.onErr,
		onStats:           options.onStats,
		telemetryProvider: options.producerTelemetryProvider,
	}

	// Poll events in a background goroutine. This has to be done or the internal queue
	// of librdkaka will overflow and cause it to crash.
	go producer.pollEvents()

	return producer, nil
}

func (p *Producer) pollEvents() {
	events := p.base.Events()
	for {
		select {
		case <-p.eventStopCh:
			return
		case event, ok := <-events:
			if !ok {
				return
			}
			switch event := event.(type) {
			case *kafka.Message:
				if event.TopicPartition.Error != nil {
					p.telemetryProvider.RecordDeliveryError(*event.TopicPartition.Topic)
				} else {
					p.telemetryProvider.RecordMessageDelivered(*event.TopicPartition.Topic)
				}
			case kafka.Error:
				p.telemetryProvider.RecordKafkaError(int(event.Code()))
			}
		}
	}
}

// M returns a MessageBuilder that provides a fluent API for building and sending
// a message.
func (p *Producer) M() *MessageBuilder {
	return &MessageBuilder{
		producer: p,
	}
}

// ProduceAsync produces a message to Kafka asynchronously. The message will be
// transmitted to the Kafka brokers, and the delivery report will be sent to the
// provided delivery channel.
//
// ProduceAsync returns immediately after the message has been queued to be
// transmitted to the Kafka brokers. The delivery report will be sent to the
// provided delivery channel when the message has been successfully transmitted
// to the Kafka brokers or an error has occurred.
//
// ProduceAsync returns an error only if the message could not be queued for delivery
// but does not indicate whether the message was successfully transmitted to the
// Kafka brokers. The caller must check the delivery report to determine if the
// message was successfully transmitted to the Kafka brokers.
//
// Note providing a nil delivery channel will cause the delivery report to be
// discarded effectively making ProduceAsync a fire and forget operation.
//
// The context is used for tracing purposes. The context being canceled or exceeding
// its deadline has no effect. This is because the underlying Confluent Kafka GO
// client does not use or respect the context.
func (p *Producer) ProduceAsync(ctx context.Context, m Message, deliveryCh chan DeliveryReport) error {
	var err error

	kafkaMessage := toKafkaMessage(m)
	kafkaMessage.TopicPartition.Partition = kafka.PartitionAny

	ctx, traceDone := p.telemetryProvider.Trace(ctx, kafkaMessage)
	defer func() {
		traceDone(err)
	}()

	var internalDeliveryCh chan kafka.Event
	if deliveryCh != nil {
		internalDeliveryCh = make(chan kafka.Event, 1)
	}

	err = p.base.Produce(kafkaMessage, internalDeliveryCh)
	if err != nil {
		p.telemetryProvider.RecordDeliveryEnqueueError(*kafkaMessage.TopicPartition.Topic)
		return WrapAsRetryable(fmt.Errorf("kafka: enqueue message: %w", err))
	}

	// If there is a delivery channel provided, a goroutine is started to read from
	// the internal delivery channel and send the result to the provided delivery as
	// to not expose the internals of the kafka client. If no delivery channel is
	// provided, the delivery report is discarded.
	if deliveryCh != nil {
		go func(ctx context.Context) {
			defer close(internalDeliveryCh)

			var err error
			_, done := p.telemetryProvider.TraceDelivery(ctx)
			defer func() {
				done(err)
			}()

			result := <-internalDeliveryCh
			switch event := result.(type) {
			case *kafka.Message:
				if event.TopicPartition.Error == nil {
					p.telemetryProvider.RecordMessageDelivered(*event.TopicPartition.Topic)
					deliveryCh <- DeliveryReport{
						Error:     nil,
						ErrorCode: 0,
						Topic:     *event.TopicPartition.Topic,
						Partition: int(event.TopicPartition.Partition),
						Offset:    int64(event.TopicPartition.Offset),
						Opaque:    event.Opaque,
					}
				} else {
					err = event.TopicPartition.Error
					p.telemetryProvider.RecordDeliveryError(*event.TopicPartition.Topic)
				}
			case kafka.Error:
				err = event
				p.telemetryProvider.RecordKafkaError(int(event.Code()))
				deliveryCh <- DeliveryReport{
					Error:     event,
					ErrorCode: int(event.Code()),
				}
			default:
				deliveryCh <- DeliveryReport{
					Error:     fmt.Errorf("kafka: unexpected event type: %T", event),
					ErrorCode: -9999,
				}
			}
		}(ctx)
	}

	return nil
}

// Produce produces a message to Kafka synchronously. The message will be queued for
// transmission to the Kafka brokers, and Produce will block until the message has been
// delivered to the Kafka brokers or an error has occurred.
//
// Produce will return an error under the following conditions:
//
// 1. The message could not be queued for delivery.
// 2. The message could not be delivered to Kafka.
// 3. The context was canceled or exceeded its deadline.
//
// It is important to note that the underlying Confluent Kafka GO client does not use
// or respect the context. If the context is canceled or exceeds its deadline, it does
// not interrupt the delivery of the message. Instead, it simply aborts waiting on the
// delivery report from Kafka. Generally speaking, the context is mainly used for tracing
// purposes.
func (p *Producer) Produce(ctx context.Context, m Message) error {
	var err error

	kafkaMessage := toKafkaMessage(m)
	kafkaMessage.TopicPartition.Partition = kafka.PartitionAny

	ctx, traceDone := p.telemetryProvider.Trace(ctx, kafkaMessage)
	defer func() {
		traceDone(err)
	}()

	deliveryCh := make(chan kafka.Event, 1)
	err = p.base.Produce(kafkaMessage, deliveryCh)
	if err != nil {
		return WrapAsRetryable(fmt.Errorf("kafka: enqueue message: %w", err))
	}

	select {
	case <-ctx.Done():
		err = ctx.Err()
		return err
	case result := <-deliveryCh:
		switch event := result.(type) {
		case *kafka.Message:
			if event.TopicPartition.Error != nil {
				err = event.TopicPartition.Error
				return fmt.Errorf("kafka: failed to deliver message: %w", event.TopicPartition.Error)
			}
			return nil
		case kafka.Error:
			err = event
			return fmt.Errorf("kafka: failed to deliver message: %w", event)
		default:
			return fmt.Errorf("kafka: unexpected event type: %T", event)
		}
	}
}

func (p *Producer) Transactional(ctx context.Context, messages []Message) error {
	if err := p.base.InitTransactions(ctx); err != nil {
		return fmt.Errorf("kafka: failed to initialize transactions: %w", err)
	}

	err := p.base.BeginTransaction()
	deliveryChan := make(chan kafka.Event, len(messages))

	for i := 0; i < len(messages); i++ {
		msg := toKafkaMessage(messages[i])
		err = p.base.Produce(msg, deliveryChan)
		if err != nil {
			p.telemetryProvider.RecordDeliveryEnqueueError(*msg.TopicPartition.Topic)
			abortErr := p.base.AbortTransaction(ctx)
			if abortErr != nil {
				return fmt.Errorf("kafka: failed to abort transaction: %w: failed to produce message: %w", abortErr, err)
			}
			return err
		}
	}

	for i := 0; i < len(messages); i++ {
		event := <-deliveryChan
		switch ev := event.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				p.telemetryProvider.RecordDeliveryError(*ev.TopicPartition.Topic)
				abortErr := p.base.AbortTransaction(ctx)
				if abortErr != nil {
					return fmt.Errorf("kafka: failed to abort transaction: %w: message delivery failed: %w", abortErr, ev.TopicPartition.Error)
				}
				return fmt.Errorf("kafka: transaction aborted: delivery failure: %w", ev.TopicPartition.Error)
			}
		}
	}
	close(deliveryChan)

	err = p.base.CommitTransaction(ctx)
	if err != nil {
		return fmt.Errorf("kafka: failed to commit transaction: %w", err)
	}

	return nil
}

// Len returns the number of messages and requests waiting to be transmitted to
// the broker as well as delivery reports queued for the application.
func (p *Producer) Len() int {
	return p.base.Len()
}

// Purge removes all messages within librdkafka's internal queue waiting to be
// transmitted to the Kafka brokers.
//
// Note that Purge does not remove any messages that have already been delivered
// to the Kafka brokers. This method should be used with extreme caution unless
// you don't care if messages are delivered or not.
func (p *Producer) Purge() error {
	return p.base.Purge(kafka.PurgeQueue)
}

// Flush and wait for outstanding messages and requests to complete delivery. Runs
// until value reaches zero or timeout is exceeded. Returns the number of outstanding
// events still un-flushed.
func (p *Producer) Flush(timeout time.Duration) int {
	return p.base.Flush(int(timeout.Milliseconds()))
}

// Close stops the producer and releases any resources. A Producer is not usable
// after this method is called.
func (p *Producer) Close() {
	p.base.Close()
}

// IsClosed returns true if the producer has been closed, otherwise false.
func (p *Producer) IsClosed() bool {
	return p.base.IsClosed()
}

// DeliveryReport represents the result of producing a method to Kafka.
//
// You must always check the Error field. If the value of Error is non-nil then
// the message was not delivered. On error scenarios the ErrorCode will be non-zero
// if an error code was available.
type DeliveryReport struct {
	Error     error
	ErrorCode int
	Topic     string
	Partition int
	Offset    int64
	Opaque    interface{}
}

func producerConfigMap(conf KafkaConfig) *kafka.ConfigMap {

	// Configure base properties/parameters
	configMap := &kafka.ConfigMap{
		"bootstrap.servers":                  strings.Join(conf.BootstrapServers, ","),
		"security.protocol":                  conf.SecurityProtocol.String(),
		"message.max.bytes":                  conf.MessageMaxBytes,
		"enable.idempotence":                 conf.Idempotence,
		"request.required.acks":              conf.RequiredAcks.value(),
		"topic.metadata.refresh.interval.ms": 300000,
		"connections.max.idle.ms":            600000,
	}

	if conf.TransactionID != "" {
		_ = configMap.SetKey("transactional.id", conf.TransactionID)
	}

	// If SSL is enabled any additional SSL configuration provided needs added
	// to the configmap
	if conf.SecurityProtocol == Ssl || conf.SecurityProtocol == SaslSsl {
		if conf.CertificateAuthorityLocation != "" {
			_ = configMap.SetKey("ssl.ca.location", conf.CertificateAuthorityLocation)
		}
		if conf.CertificateLocation != "" {
			_ = configMap.SetKey("ssl.certificate.location", conf.CertificateLocation)
		}
		if conf.CertificateKeyLocation != "" {
			_ = configMap.SetKey("ssl.key.location", conf.CertificateKeyLocation)
		}
		if conf.CertificateKeyPassword != "" {
			_ = configMap.SetKey("ssl.key.password", conf.CertificateKeyPassword)
		}
		if conf.SkipTlsVerification {
			_ = configMap.SetKey("enable.ssl.certificate.verification", false)
		}
	}

	// If using SASL authentication add additional SASL configuration to the
	// configmap
	if conf.SecurityProtocol == SaslPlaintext || conf.SecurityProtocol == SaslSsl {
		_ = configMap.SetKey("sasl.mechanism", conf.SASLMechanism.String())
		_ = configMap.SetKey("sasl.username", conf.SASLUsername)
		_ = configMap.SetKey("sasl.password", conf.SASLPassword)
	}

	return configMap
}
