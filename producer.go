package shiva

import (
	"context"
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

func (p *Producer) Produce(m Message, deliveryCh <-chan DeliveryReport) error {
	// todo: implement me
	return nil
}

func (p *Producer) ProduceAndWait(m Message) error {
	// todo: implement me
	return nil
}

// Len returns the number of messages and requests waiting to be transmitted to
// the broker as well as delivery reports queued for the application.
func (p *Producer) Len() int {
	return p.base.Len()
}

// Purge removes all messages within librdkafka's internal queue waiting to be
// transmitted to the Kafka brokers.
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

func (p *Producer) produce() {
	// todo: implement me, if needed, not sure yet
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
