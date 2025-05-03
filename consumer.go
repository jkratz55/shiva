package shiva

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type kafkaConsumer interface {
	Assignment() ([]kafka.TopicPartition, error)
	Subscription() (topics []string, err error)
	Committed(partitions []kafka.TopicPartition, timeoutMs int) (offsets []kafka.TopicPartition, err error)
	QueryWatermarkOffsets(topic string, partition int32, timeoutMs int) (low, high int64, err error)
	GetWatermarkOffsets(topic string, partition int32) (low, high int64, err error)
	Subscribe(topics string, rebalanceCb kafka.RebalanceCb) error
	Poll(timeoutMs int) (event kafka.Event)
	CommitMessage(msg *kafka.Message) ([]kafka.TopicPartition, error)
	StoreMessage(msg *kafka.Message) (storedOffsets []kafka.TopicPartition, err error)
	Commit() ([]kafka.TopicPartition, error)
	Position(partitions []kafka.TopicPartition) (offsets []kafka.TopicPartition, err error)
	Pause(partitions []kafka.TopicPartition) (err error)
	Resume(partitions []kafka.TopicPartition) (err error)
	IsClosed() bool
	Close() error
}

var _ kafkaConsumer = &kafka.Consumer{}

// Consumer provides a high-level API for consuming messages from Kafka.
//
// Consumer is a wrapper around the official Confluent Kafka GO client, which
// is a wrapper around librdkafka.
//
// The zero-value for Consumer is not usable. Instances of Consumer should always
// be created/initialized using NewConsumer.
type Consumer struct {
	baseConsumer       kafkaConsumer
	handler            Handler
	dlHandler          DeadLetterHandler
	topic              string
	running            bool
	mu                 sync.Mutex
	logger             Logger
	pollTimeout        int
	config             *KafkaConfig
	stopChan           chan struct{}
	onErr              func(err error)
	onAssigned         func(tps TopicPartitions)
	onRevoked          func(tps TopicPartitions)
	onStats            func(stats map[string]any)
	onOffsetsCommitted func(offsets TopicPartitions, err error)
	name               string
	telemetryProvider  ConsumerTelemetryProvider

	rebalanceCh      chan struct{}
	lagMonitorStopCh chan struct{}
}

// NewConsumer creates and initializes a Consumer instance.
//
// NewConsumer initializes a Consumer and subscribes to the provided topic. NewConsumer
// does not start polling from Kafka, thus after initialization the Consumer will not
// join the consumer group and have topics/partitions assigned.
func NewConsumer(conf KafkaConfig, topic string, handler Handler, opts ...ConsumerOption) (*Consumer, error) {
	if handler == nil {
		return nil, errors.New("invalid config: cannot initialize Consumer with nil Handler")
	}
	if strings.TrimSpace(topic) == "" {
		return nil, errors.New("invalid config: cannot initialize Consumer with empty topic")
	}

	// Initialize the defaults for the KafkaConfig instance
	conf.init()

	// Apply any options provided
	baseOpts := make([]baseOption, len(opts))
	for i, opt := range opts {
		baseOpts[i] = opt
	}
	options := newOptions(baseOpts...)

	configMap := consumerConfigMap(conf)

	stopChan := make(chan struct{})
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
			case <-stopChan:
				return
			}
		}
	}(options.logger)

	// Configure logs from librdkafka to be sent to our logger rather than stdout
	_ = configMap.SetKey("go.logs.channel.enable", true)
	_ = configMap.SetKey("go.logs.channel", logChan)

	// If a stats callback and interval are configured enable them for the Confluent/librdkafka client
	if options.statsEnabled && options.statsInterval > 0 {
		_ = configMap.SetKey("statistics.interval.ms", options.statsInterval)
	}

	base, err := kafka.NewConsumer(configMap)
	if err != nil {
		return nil, fmt.Errorf("kafka: failed to initialize Confluent Kafka Consumer: %w", err)
	}

	// If name isn't set use the topic and type of the handler to generate a default name.
	name := options.name
	if strings.TrimSpace(name) == "" {
		name = fmt.Sprintf("%s|%T", topic, handler)
	}

	consumer := &Consumer{
		baseConsumer:       base,
		handler:            handler,
		dlHandler:          options.deadLetterHandler,
		topic:              topic,
		running:            false,
		mu:                 sync.Mutex{},
		logger:             options.logger,
		pollTimeout:        int(conf.PollTimeout.Milliseconds()),
		config:             &conf,
		stopChan:           stopChan,
		onErr:              options.onErr,
		onAssigned:         options.onAssigned,
		onRevoked:          options.onRevoked,
		onStats:            options.onStats,
		onOffsetsCommitted: options.onOffsetsCommitted,
		name:               name,
		telemetryProvider:  options.consumerTelemetryProvider,
		rebalanceCh:        make(chan struct{}, 1),
		lagMonitorStopCh:   make(chan struct{}, 1),
	}

	err = consumer.baseConsumer.Subscribe(consumer.topic, consumer.onRebalance)
	if err != nil {
		return nil, fmt.Errorf("subsribe topic %s: %w", consumer.topic, err)
	}

	return consumer, nil
}

// Run polls events from Kafka until either Close is called or the Consumer encounters
// a fatal error from which it cannot recover.
//
// Run is blocking and will almost always be called in a separate goroutine. When Close
// is invoked or a fatal error occurs, the Consumer will attempt to commit any uncommitted
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

	c.running = true
	go c.monitorLag()

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
				c.lagMonitorStopCh <- struct{}{}
				return err
			}

		case *kafka.Message:
			c.handleMessage(event)

		case kafka.OffsetsCommitted:
			c.handleOffsetsCommitted(event)

		case kafka.Stats:
			c.handleStats(event)

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
	c.lagMonitorStopCh <- struct{}{}
	return closeErr
}

// handleError invokes the onErr callback and returns an error if the error is
// considered fatal, otherwise returns nil.
func (c *Consumer) handleError(err kafka.Error) error {

	c.telemetryProvider.RecordKafkaError(c.name, c.topic, int(err.Code()))

	// Invoke onErr callback
	c.onErr(fmt.Errorf("kafka: %w", err))

	if err.IsFatal() {
		return fmt.Errorf("kafka client fatal error: %w", err)
	}

	return nil
}

// handleMessage accepts a message event polled from Kafka, handles acknowledging
// the message by storing the offset locally based on the AcknowledgmentStrategy,
// and hands the Message off to the Handler.
func (c *Consumer) handleMessage(msg *kafka.Message) {
	defer func() {
		c.telemetryProvider.RecordMessageProcessed(c.name, c.topic)
	}()

	// Note: If the AcknowledgmentStrategy is set to none the message is never acknowledged on
	// purpose.

	// If the AcknowledgmentStrategy is pre-processing the message is acknowledged before the Handler
	// is invoked. This follows the at-most-once delivery model.
	if c.config.AcknowledgmentStrategy == AcknowledgmentStrategyPreProcessing {
		_, err := c.baseConsumer.StoreMessage(msg)
		if err != nil {
			c.onErr(fmt.Errorf("acknowledge message: topic %s, partition %d offset %d: %w",
				*msg.TopicPartition.Topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset, err))
		}
	}

	startTs := time.Now()

	shivaMsg := mapMessage(msg)
	err := c.handler.Handle(shivaMsg)

	handlerDur := time.Since(startTs)
	c.telemetryProvider.RecordHandlerExecutionDuration(c.name, c.topic, handlerDur)

	if err != nil {
		c.telemetryProvider.RecordHandlerError(c.name, c.topic)

		// If the Handler returns an error signaling, the message was not successfully
		// processed. The DeadLetterHandler is invoked to provide a last opportunity
		// to do something with the message before the Consumer moves on to the next
		// Message.
		c.dlHandler.Handle(shivaMsg, err)
	}

	// If the AcknowledgmentStrategy is post-processing the message is acknowledged after the Handler
	// is invoked, regardless if it returns an error value. This follows the at-least-once delivery
	// model.
	if c.config.AcknowledgmentStrategy == AcknowledgmentStrategyPostProcessing {
		_, err := c.baseConsumer.StoreMessage(msg)
		if err != nil {
			c.onErr(fmt.Errorf("acknowledge message: topic %s, partition %d offset %d: %w",
				*msg.TopicPartition.Topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset, err))
		}
	}
}

// handleOffsetsCommitted accepts OffsetsCommitted event from the kafka client and maps
// it to internal Shiva representation before invoking the onOffsetsCommitted callback.
func (c *Consumer) handleOffsetsCommitted(offsets kafka.OffsetsCommitted) {
	tps := mapTopicPartitions(offsets.Offsets)
	c.onOffsetsCommitted(tps, offsets.Error)
}

// handleStats unmarshalls the raw json string from the kafka client and returns the
// stats as a map.
func (c *Consumer) handleStats(event kafka.Stats) {
	var stats map[string]any
	err := json.Unmarshal([]byte(event.String()), &stats)
	if err != nil {
		// This should never happen, but in case it does call the onErr callback
		// if the user wants the error
		c.onErr(fmt.Errorf("unmarshal kafka stats: %w", err))
	}
	c.onStats(stats)
}

// Assignment returns the topics and partitions currently assigned to the Consumer.
//
// Note that until the Consumer is running and polling, the Consumer won't have joined the group
// and won't have any assignments.
func (c *Consumer) Assignment() (TopicPartitions, error) {
	tps, err := c.baseConsumer.Assignment()
	if err != nil {
		return nil, err
	}
	return mapTopicPartitions(tps), nil
}

// Subscription returns the current topic the Consumer is subscribed to.
func (c *Consumer) Subscription() ([]string, error) {
	return c.baseConsumer.Subscription()
}

// Position returns the next Offset the Consumer will read from for each topic/partition assigned
// to the Consumer.
//
// Note that until the Consumer is running and polling, the Consumer won't have joined the group
// and won't have any assignments. Additionally, the offsets returned are based on the local state
// of the client and not what has been committed to Kafka.
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

// Commit commits the highest currently stored offsets for each topic/partition back to Kafka.
func (c *Consumer) Commit() (TopicPartitions, error) {
	tps, err := c.baseConsumer.Commit()
	if err != nil {
		return nil, err
	}
	return mapTopicPartitions(tps), nil
}

// Committed queries the Kafka brokers to get the latest committed offsets for the topics/partitions
// assigned to the Consumer.
func (c *Consumer) Committed(timeout time.Duration) (TopicPartitions, error) {
	assignment, err := c.baseConsumer.Assignment()
	if err != nil {
		return nil, fmt.Errorf("get assignment: %w", err)
	}

	committedOffsets, err := c.baseConsumer.Committed(assignment, int(timeout.Milliseconds()))
	if err != nil {
		return nil, err
	}

	return mapTopicPartitions(committedOffsets), nil
}

// Pause consumption for the topics and partitions currently assigned to the Consumer.
//
// Note that messages already enqueued on the consumer's Event channel will NOT be purged by this
// call and will still be processed. Additionally, if a rebalance is triggered, the Consumer will
// resume fetching/polling from Kafka as the pause state is not persisted between rebalances.
func (c *Consumer) Pause() error {
	assignment, err := c.baseConsumer.Assignment()
	if err != nil {
		return fmt.Errorf("pause: get assignment: %w", err)
	}

	return c.baseConsumer.Pause(assignment)
}

// Resume resumes consuming the topics/partitions previously paused that are assigned to the
// Consumer instance.
//
// Note that calling Resume on topics/partitions that are not paused is a no-op.
func (c *Consumer) Resume() error {
	assignment, err := c.baseConsumer.Assignment()
	if err != nil {
		return fmt.Errorf("resume: get assignment: %w", err)
	}
	return c.baseConsumer.Resume(assignment)
}

// Lag returns the current lag for each topic/partition assigned to the Consumer.
//
// The lag is returned as a map[topic|partition]offset.
func (c *Consumer) Lag() (map[string]int64, error) {
	assignments, err := c.baseConsumer.Assignment()
	if err != nil {
		return nil, fmt.Errorf("lag: get assignment: %w", err)
	}

	lag := make(map[string]int64)

	positions, err := c.baseConsumer.Position(assignments)
	if err != nil {
		return nil, fmt.Errorf("get position: %w", err)
	}

	for _, tp := range positions {
		_, high, err := c.baseConsumer.GetWatermarkOffsets(*tp.Topic, tp.Partition)
		if err != nil {
			return lag, fmt.Errorf("get watermark offsets: %w", err)
		}

		key := fmt.Sprintf("%s|%d", *tp.Topic, tp.Partition)
		currentOffset := int64(tp.Offset)
		if currentOffset < 0 {
			currentOffset = 0
		}

		partitionLag := high - currentOffset
		if partitionLag < 0 {
			partitionLag = 0
		}

		lag[key] = partitionLag
	}

	return lag, nil
}

// GetWatermarkOffsets gets the lowest and highest offsets for each partition currently
// assigned to the Consumer.
//
// The watermarks are returned as a map where the key is topic|partition.
//
// Note that GetWatermarkOffsets return the watermarks based on the local client state.
// This is fine for most use-cases but if you need the absolute most up-to-date watermarks,
// use QueryWatermarkOffsets instead as it queries the Kafka brokers.
func (c *Consumer) GetWatermarkOffsets() (map[string]Watermark, error) {
	assignment, err := c.baseConsumer.Assignment()
	if err != nil {
		return nil, fmt.Errorf("get assignment: %w", err)
	}

	watermarks := make(map[string]Watermark)
	for _, tp := range assignment {
		low, high, waterMarkErr := c.baseConsumer.GetWatermarkOffsets(*tp.Topic, tp.Partition)
		if waterMarkErr != nil {
			return nil, fmt.Errorf("get watermark offset: topic %s partition %d: %w",
				*tp.Topic, tp.Partition, waterMarkErr)
		}

		key := fmt.Sprintf("%s|%d", *tp.Topic, tp.Partition)
		watermarks[key] = Watermark{
			Low:  low,
			High: high,
		}
	}

	return watermarks, nil
}

// QueryWatermarkOffsets fetches the lowest and highest offsets for each partition currently
// assigned to the Consumer from the Kafka brokers.
//
// The watermarks are returned as a map where the key is topic|partition.
func (c *Consumer) QueryWatermarkOffsets(ctx context.Context) (map[string]Watermark, error) {
	assignment, err := c.baseConsumer.Assignment()
	if err != nil {
		return nil, fmt.Errorf("get assignment: %w", err)
	}

	errCh := make(chan error, 1)
	resultCh := make(chan map[string]Watermark, 1)

	go func() {
		watermarks := make(map[string]Watermark)

		for _, tp := range assignment {
			if ctx.Err() != nil {
				errCh <- ctx.Err()
				return
			}

			low, high, err := c.baseConsumer.QueryWatermarkOffsets(*tp.Topic, tp.Partition, 30000)
			if err != nil {
				errCh <- fmt.Errorf("query watermark offset: topic %s partition %d: %w",
					*tp.Topic, tp.Partition, err)
				return
			}

			key := fmt.Sprintf("%s|%d", *tp.Topic, tp.Partition)
			watermarks[key] = Watermark{
				Low:  low,
				High: high,
			}
		}

		resultCh <- watermarks
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-errCh:
		return nil, err
	case result := <-resultCh:
		return result, nil
	}
}

// IsRunning indicates if the Consumer is running.
func (c *Consumer) IsRunning() bool {
	return c.running
}

// IsClosed indicates if the Consumer is closed.
func (c *Consumer) IsClosed() bool {
	return c.baseConsumer.IsClosed()
}

// Close stops the Consumer and releases the underlying resources.
//
// After Close the Consumer is not usable.
func (c *Consumer) Close() {
	c.running = false
}

func (c *Consumer) onRebalance(_ *kafka.Consumer, event kafka.Event) error {
	c.telemetryProvider.RecordRebalance(c.name, c.config.GroupID)

	switch e := event.(type) {
	case kafka.AssignedPartitions:
		c.onAssigned(mapTopicPartitions(e.Partitions))
	case kafka.RevokedPartitions:
		c.onRevoked(mapTopicPartitions(e.Partitions))
	}

	// Notify goroutine monitoring lag to reset its gauges to 0
	c.rebalanceCh <- struct{}{}

	return nil
}

// monitorLag updates metrics with the current lag every 10 seconds.
func (c *Consumer) monitorLag() {
	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()

	knownTopicPartitions := make(map[string]struct{})

	for {
		select {
		case <-ticker.C:
			// If the Consumer is no longer running return
			if !c.running {
				return
			}

			lags, err := c.Lag()
			if err != nil {
				c.logger.Warn("Failed for update lag metrics",
					"err", err)
			}

			for tp, lag := range lags {
				knownTopicPartitions[tp] = struct{}{}
				parts := strings.Split(tp, "|")
				c.telemetryProvider.RecordLag(c.name, c.config.GroupID, parts[0], parts[1], lag)
			}

		case <-c.rebalanceCh:
			// If a rebalance occurs, we may have end up with different partitions or maybe even
			// no partitions. This means the metrics (gauges) we've been updating with the lag
			// could remain fixed and create misleading metrics. When we detect a rebalance, we
			// reset the lag to the partitions we've been updating to 0
			for tp, _ := range knownTopicPartitions {
				parts := strings.Split(tp, "|")
				topic := parts[0]
				partition := parts[1]

				c.telemetryProvider.RecordLag(c.name, c.config.GroupID, topic, partition, 0)
			}
			knownTopicPartitions = make(map[string]struct{})

		case <-c.lagMonitorStopCh:
			// Consumer is done, stop monitoring lag
			return
		}
	}
}

// consumerConfigMap maps the configuration to the ConfigMap the Confluent Kafka
// Go client expects.
func consumerConfigMap(conf KafkaConfig) *kafka.ConfigMap {
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

	// If using SASL authentication, add additional SASL configuration to the
	// configmap
	if conf.SecurityProtocol == SaslPlaintext || conf.SecurityProtocol == SaslSsl {
		_ = configMap.SetKey("sasl.mechanism", conf.SASLMechanism.String())
		_ = configMap.SetKey("sasl.username", conf.SASLUsername)
		_ = configMap.SetKey("sasl.password", conf.SASLPassword)
	}

	return configMap
}
