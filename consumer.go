package shiva

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type kafkaConsumer interface {
	Assignment() ([]kafka.TopicPartition, error)
	Subscription() (topics []string, err error)
	Committed(partitions []kafka.TopicPartition, timeoutMs int) (offsets []kafka.TopicPartition, err error)
	QueryWatermarkOffsets(topic string, partition int32, timeoutMs int) (low, high int64, err error)
	Subscribe(topics string, rebalanceCb kafka.RebalanceCb) error
	Poll(timeoutMs int) (event kafka.Event)
	CommitMessage(msg *kafka.Message) ([]kafka.TopicPartition, error)
	StoreMessage(msg *kafka.Message) (storedOffsets []kafka.TopicPartition, err error)
	Commit() ([]kafka.TopicPartition, error)
	Position(partitions []kafka.TopicPartition) (offsets []kafka.TopicPartition, err error)
	IsClosed() bool
	Close() error
}

var _ kafkaConsumer = &kafka.Consumer{}

type Consumer struct {
	baseConsumer kafkaConsumer
	handler      Handler
	topic        string
	running      bool
	mu           sync.Mutex
	logger       Logger
	pollTimeout  int
	config       *Config
	stopChan     chan struct{}
}

func NewConsumer(conf Config, topic string, handler Handler) (*Consumer, error) {
	if handler == nil {
		return nil, errors.New("invalid config: cannot initialize Consumer with nil Handler")
	}
	if strings.TrimSpace(topic) == "" {
		return nil, errors.New("invalid config: cannot initialize Consumer with empty topic")
	}

	conf.init()

	configMap := consumerConfigMap(conf)

	stopChan := make(chan struct{})
	logChan := make(chan kafka.LogEvent, 1000)

	// Start goroutine to read logs from librdkafka and uses slog.Logger to log
	// them rather than dumping them to stdout
	go func(logger Logger) {
		for {
			select {
			case logEvent, ok := <-logChan:
				if !ok {
					return
				}
				logger.Debug(logEvent.Message,
					slog.Group("librdkafka",
						slog.String("name", logEvent.Name),
						slog.String("tag", logEvent.Tag),
						slog.Int("level", logEvent.Level)))
			case <-stopChan:
				return
			}
		}
	}(conf.Logger)

	// Configure logs from librdkafka to be sent to our logger rather than stdout
	_ = configMap.SetKey("go.logs.channel.enable", true)
	_ = configMap.SetKey("go.logs.channel", logChan)

	base, err := kafka.NewConsumer(configMap)
	if err != nil {
		return nil, fmt.Errorf("kafka: failed to initialize Confluent Kafka Consumer: %w", err)
	}

	consumer := &Consumer{
		baseConsumer: base,
		handler:      handler,
		topic:        topic,
		config:       &conf,
		stopChan:     stopChan,
		logger:       conf.Logger,
		pollTimeout:  int(conf.PollTimeout.Milliseconds()),
		mu:           sync.Mutex{},
		running:      false,
	}
	return consumer, nil
}

// Run polls events from Kafka until either Close is called or the Consumer encounters
// a fatal error from which it cannot recover.
//
// Run is blocking and will almost always be called in a separate goroutine. When Close
// is invoked or a fatal error occurs the Consumer will attempt to commit an uncommitted
// offsets back to the Kafka brokers. If the Consumer encountered a fatal error or failed
// to commit offsets while closing a non-nil error value will be returned.
func (c *Consumer) Run() error {

	// Try to acquire the lock to ensure Run cannot be invoked more than once on
	// the same Consumer instance. If the lock cannot be acquired, return an error
	// as that means the Consumer is already running.
	if ok := c.mu.TryLock(); !ok {
		return fmt.Errorf("unsupported operation: cannot invoke Run() on an already running Consumer")
	}
	defer c.mu.Unlock()

	// Once a Consumer is closed it cannot be reused.
	if c.baseConsumer.IsClosed() {
		return errors.New("unsupported operation: cannot invoke Run() on a closed Consumer")
	}

	err := c.baseConsumer.Subscribe(c.topic, c.onRebalance)
	if err != nil {
		return fmt.Errorf("failed to subscribe to topic %s: %w", c.topic, err)
	}

	c.running = true

	// Event loop that continuously polls the brokers for new events and handles
	// them accordingly. The loop will continue to run until the Consumer is closed
	// or encounters a fatal error.
	for c.running {
		rawEvent := c.baseConsumer.Poll(c.pollTimeout)

		switch event := rawEvent.(type) {
		case kafka.Error:

			// Handle the error event polled from Kafka. The handleError method
			// will only bubble up an error if Confluent Kafka Go/librdkafka client
			// returns a fatal error indicating the Consumer cannot continue. In this
			// case the consumer attempts to commit the offsets back to Kafka, but it
			// is unlikely to succeed. The Consumer is then closed and the error is
			// bubbled up to the caller to indicate the Consumer has unexpectedly
			// stopped due to a fatal error and cannot continue.
			if err := c.handleError(event); err != nil {
				c.running = false
				_, _ = c.baseConsumer.Commit()
				_ = c.baseConsumer.Close()
				c.stopChan <- struct{}{}
				return err
			}

		case *kafka.Message:
			c.handleMessage(event)

		case kafka.OffsetsCommitted:
			c.handleOffsetsCommitted(event)
		default:
			c.logger.Debug(fmt.Sprintf("Ignoring event %T", event))
		}
	}

	// The Consumer was gracefully closed using Close() method. Commit any offsets
	// that have not been committed yet, and close the underlying Confluent Kafka
	// client to release resources. Any errors that occur during the commit or close
	// will be bubbled up to the caller.
	var closeErr error
	if _, err := c.baseConsumer.Commit(); err != nil {
		closeErr = err
	}
	if err := c.baseConsumer.Close(); err != nil {
		closeErr = errors.Join(closeErr, err)
	}

	// Signal to the goroutine processing logs to stop
	c.stopChan <- struct{}{}
	return closeErr
}

func (c *Consumer) handleError(err kafka.Error) error {
	// todo: implement
}

func (c *Consumer) handleMessage(msg *kafka.Message) {
	// Note: If the AcknowledgmentStrategy is set to none the message is never acknowledged on
	// purpose.

	// If the AcknowledgmentStrategy is pre-processing the message is acknowledged before the Handler
	// is invoked. This follows the at-most-once delivery model.
	if c.config.AcknowledgmentStrategy == AcknowledgmentStrategyPreProcessing {
		_, err := c.baseConsumer.StoreMessage(msg)
		if err != nil {
			c.logger.Error("Failed to acknowledge message by storing the offset",
				slog.String("err", err.Error()))
		}
	}

	err := c.handler.Handle(mapMessage(msg))
	if err != nil {
		// todo: log failure
	}

	// If the AcknowledgmentStrategy is post-processing the message is acknowledged after the Handler
	// is invoked, regardless if it returns an error value. This follows the at-least-once delivery
	// model.
	if c.config.AcknowledgmentStrategy == AcknowledgmentStrategyPostProcessing {
		_, err := c.baseConsumer.StoreMessage(msg)
		if err != nil {
			// todo: handle error
		}
	}
}

func (c *Consumer) handleOffsetsCommitted(offsets kafka.OffsetsCommitted) {
	// todo: log results
}

func (c *Consumer) Assignment() (TopicPartitions, error) {
	tps, err := c.baseConsumer.Assignment()
	if err != nil {
		return nil, err
	}
	return mapTopicPartitions(tps), nil
}

func (c *Consumer) Subscription() ([]string, error) {
	return c.baseConsumer.Subscription()
}

func (c *Consumer) Position() (TopicPartitions, error) {
	topicPartitions, err := c.baseConsumer.Assignment()
	if err != nil {
		return nil, fmt.Errorf("failed to get assignments for Consumer: %w", err)
	}

	tps, err := c.baseConsumer.Position(topicPartitions)
	if err != nil {
		return nil, nil
	}
	return mapTopicPartitions(tps), nil
}

func (c *Consumer) Commit() (TopicPartitions, error) {
	tps, err := c.baseConsumer.Commit()
	if err != nil {
		return nil, err
	}
	return mapTopicPartitions(tps), nil
}

func (c *Consumer) IsRunning() bool {
	return c.running
}

func (c *Consumer) IsClosed() bool {
	return c.baseConsumer.IsClosed()
}

func (c *Consumer) Close() {
	c.running = false
}

func (c *Consumer) onRebalance(_ *kafka.Consumer, event kafka.Event) error {
	switch e := event.(type) {
	case kafka.AssignedPartitions:
		// todo: log event
	case kafka.RevokedPartitions:
		// todo: log event
	}

	return nil
}

// consumerConfigMap maps the configuration to the ConfigMap the Confluent Kafka
// Go client expects.
func consumerConfigMap(conf Config) *kafka.ConfigMap {
	// Configure base properties/parameters
	configMap := &kafka.ConfigMap{
		"bootstrap.servers":                  strings.Join(conf.BootstrapServers, ","),
		"group.id":                           conf.GroupID,
		"session.timeout.ms":                 int(conf.SessionTimeout.Milliseconds()),
		"heartbeat.interval.ms":              int(conf.HeartbeatInterval.Milliseconds()),
		"auto.offset.reset":                  conf.AutoOffsetReset.String(),
		"enable.auto.offset.store":           false,
		"auto.commit.interval.ms":            int(conf.CommitInterval.Milliseconds()),
		"security.protocol":                  conf.SecurityProtocol.String(),
		"message.max.bytes":                  conf.MessageMaxBytes,
		"fetch.max.bytes":                    conf.MaxFetchBytes,
		"topic.metadata.refresh.interval.ms": 300000,
		"connections.max.idle.ms":            600000,
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
