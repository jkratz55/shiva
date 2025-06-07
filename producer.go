package shiva

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type baseProducer interface {
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

var _ baseProducer = &kafka.Producer{}

type Producer struct {
	base         baseProducer
	loggerStopCh chan struct{}
	eventStopCh  chan struct{}

	// callbacks
	onMessageDelivered func(report DeliveryReport)
	onErr              func(err error)
	onStats            func(stats map[string]any)

	// telemetry
	telemetryProvider ProducerTelemetryProvider
}

func NewProducer(config KafkaConfig) *Producer {
	// todo: implement me
	return &Producer{}
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

	_, traceDone := p.telemetryProvider.Trace(ctx, kafkaMessage)
	defer func() {
		traceDone(err)
	}()

	var internalDeliveryCh chan kafka.Event
	if deliveryCh != nil {
		internalDeliveryCh = make(chan kafka.Event, 1)
	}

	err = p.base.Produce(kafkaMessage, internalDeliveryCh)
	if err != nil {
		return WrapAsRetryable(fmt.Errorf("kafka: enqueue message: %w", err))
	}

	// If there is a delivery channel provided, a goroutine is started to read from
	// the internal delivery channel and send the result to the provided delivery as
	// to not expose the internals of the kafka client. If no delivery channel is
	// provided, the delivery report is discarded.
	if deliveryCh != nil {
		go func() {
			defer close(internalDeliveryCh)
			result := <-internalDeliveryCh
			switch event := result.(type) {
			case *kafka.Message:
				deliveryCh <- DeliveryReport{
					Error:     nil,
					ErrorCode: 0,
					Topic:     *event.TopicPartition.Topic,
					Partition: int(event.TopicPartition.Partition),
					Offset:    int64(event.TopicPartition.Offset),
					Opaque:    event.Opaque,
				}
			case kafka.Error:
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
		}()
	}

	// --------------------------------------------------------------------------------------------
	// todo: extract and abstract otel tracing code

	// ctx, span := otel.Tracer("shiva").Start(ctx, "kafka.produce")
	// defer span.End()
	//
	// headers := shivaotel.KafkaHeaderCarrier{}
	// otel.GetTextMapPropagator().Inject(ctx, &headers)
	//
	// mappedMessage := toKafkaMessage(m)
	// mappedMessage.Headers = append(mappedMessage.Headers, []kafka.Header(headers)...)
	//
	// err := p.base.Produce(mappedMessage, nil)
	//
	// if err != nil {
	// 	span.RecordError(err)
	// 	return err
	// }

	// --------------------------------------------------------------------------------------------

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
		return ctx.Err()
	case result := <-deliveryCh:
		switch event := result.(type) {
		case *kafka.Message:
			if event.TopicPartition.Error != nil {
				return fmt.Errorf("kafka: failed to deliver message: %w", event.TopicPartition.Error)
			}
			return nil
		case kafka.Error:
			return fmt.Errorf("kafka: failed to deliver message: %w", event)
		default:
			return fmt.Errorf("kafka: unexpected event type: %T", event)
		}
	}
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
		"bootstrap.servers": strings.Join(conf.BootstrapServers, ","),
		"security.protocol": conf.SecurityProtocol.String(),
		"message.max.bytes": conf.MessageMaxBytes,
		// "enable.idempotence":                 conf.Idempotence, // fixme: needs added to config
		"request.required.acks":              conf.RequiredAcks.value(),
		"topic.metadata.refresh.interval.ms": 300000,
		"connections.max.idle.ms":            600000,
	}

	// fixme: needs addressed in config
	// if conf.TransactionID != "" {
	// 	_ = configMap.SetKey("transactional.id", conf.TransactionID)
	// }

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
