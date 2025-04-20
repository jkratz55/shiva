package shiva

import (
	"context"
	"time"

	"github.com/sethvargo/go-envconfig"
)

// KafkaConfig represents the configuration settings for a Kafka client, including connection,
// authentication, and behavior options.
type KafkaConfig struct {
	// The Kafka brokers addresses used to establish the initial connection to
	// Kafka. This is a required field.
	//
	// Applies To: Consumer, Producer
	BootstrapServers []string `env:"SHIVA_KAFKA_BOOTSTRAP_SERVERS,required"`

	// The ID of the consumer group to join. This is a required field when using
	// Consumer.
	//
	// Applies To: Consumer
	GroupID string `env:"SHIVA_KAFKA_GROUP_ID"`

	// Client group session and failure detection timeout. The consumer sends
	// periodic heartbeats to indicate its liveness to the broker. If no heart
	// beats are received by the broker for a group member within the session
	// timeout, the broker will remove the consumer from the group and trigger
	// a rebalance.
	//
	// The unit is milliseconds with a default of 45000 (45 seconds).
	//
	// Applies To: Consumer
	SessionTimeout time.Duration `env:"SHIVA_KAFKA_SESSION_TIMEOUT,default=45s"`

	// Interval at which the consumer sends heartbeats to the broker. The default
	// is 3000 (3 seconds).
	//
	// Applies To: Consumer
	HeartbeatInterval time.Duration `env:"SHIVA_KAFKA_HEARTBEAT_TIMEOUT,default=3s"`

	// The interval between committing offsets back to Kafka brokers. The default
	// is 5000ms (5 seconds).
	//
	// Applies To: Consumer
	CommitInterval time.Duration `env:"SHIVA_KAFKA_COMMIT_INTERVAL,default=5s"`

	// The amount of time to wait for an event when polling from Kafka. The default
	// 100ms.
	//
	// Applies To: Consumer
	PollTimeout time.Duration `env:"SHIVA_KAFKA_POLL_TIMEOUT,default=100ms"`

	// Configures the behavior when there are no stored offsets found for the
	// Consumer group for topic/partition.
	//
	// The default is latest which means that the consumer will start reading
	// from the latest message in the topic/partition.
	//
	// Applies To: Consumer
	AutoOffsetReset AutoOffsetReset `env:"SHIVA_KAFKA_AUTO_OFFSET_RESET,default=earliest"`

	// Configures when the Consumer acknowledges the message. The possible values
	// are:
	//
	//  none - Messages will not be acknowledged and no offsets will be tracked
	//  pre-processing - Messages will be acknowledged when received but before
	//                   the Handler has been invoked. This follows the at-most-once
	//                   delivery model.
	//  post-processing - Messages will be acknowledged only after the Handler has
	//                    returned, regardless if it succeeded or returned an error.
	//                    This follows the at-least-once delivery model.
	//
	// The default is post-processing.
	AcknowledgmentStrategy AcknowledgmentStrategy `env:"SHIVA_KAFKA_CONSUMER_ACK_STRATEGY,default=post-processing"`

	// The maximum size for a message. The default is 1048576 (1MB).
	//
	// Applies To: Consumer, Producer
	MessageMaxBytes int `env:"SHIVA_KAFKA_MESSAGE_MAX_BYTES,default=1048576"`

	// Maximum amount of data the broker shall return for a Fetch request. Messages
	// are fetched in batches by the consumer. The default is 52428800 (50MB).
	//
	// Applies To: Consumer
	MaxFetchBytes int `env:"KAFKA_MAX_FETCH_BYTES, default=52428800"`

	// The security protocol used to communicate with the brokers.
	//
	// Valid values are: plaintext, ssl, sasl_plaintext, sasl_ssl.
	//
	// Applies To: Consumer, Producer
	SecurityProtocol SecurityProtocol `env:"SHIVA_KAFKA_SECURITY_PROTOCOL,default=plaintext"`

	// The location of the certificate authority file used to verify the brokers.
	//
	// Applies To: Consumer, Producer
	CertificateAuthorityLocation string `env:"SHIVA_KAFKA_CERT_AUTHORITY_LOCATION"`

	// The location of the client certificate used to authenticate with the brokers.
	//
	// Applies To: Consumer, Producer
	CertificateLocation string `env:"SHIVA_KAFKA_CERT_LOCATION"`

	// The location of the key for the client certificate.
	//
	// Applies To: Consumer, Producer
	CertificateKeyLocation string `env:"SHIVA_KAFKA_CERT_KEY_LOCATION"`

	// The password for the key used for the client certificate.
	//
	// Applies To: Consumer, Producer
	CertificateKeyPassword string `env:"SHIVA_KAFKA_CERT_KEY_PASSWORD"`

	// Skip TLS verification when using SSL or SASL_SSL.
	//
	// Applies To: Consumer, Producer
	SkipTlsVerification bool `env:"SHIVA_KAFKA_SKIP_TLS_VERIFICATION,default=false"`

	// The SASL mechanism to use for SASL authentication.
	//
	// Applies To: Consumer, Producer
	SASLMechanism SaslMechanism `env:"SHIVA_KAFKA_SASL_MECHANISM,default=PLAIN"`

	// The username for authenticating with SASL.
	//
	// Applies To: Consumer, Producer
	SASLUsername string `env:"SHIVA_KAFKA_SASL_USERNAME"`

	// The password for authenticating with SASL.
	//
	// Applies To: Consumer, Producer
	SASLPassword string `env:"SHIVA_KAFKA_SASL_PASSWORD"`

	// The number of acknowledgements the producer requires the leader to have
	// received before considering a request complete. The default value is
	// AckAll ("all"), which will wait for all in-sync replicas to acknowledge
	// before proceeding.
	//
	// Applies To: Producer
	RequiredAcks RequiredAck `env:"SHIVA_KAFKA_PRODUCER_REQUIRED_ACKS, default=all"`

	// Configures the logger used by the Consumer and Producer types.
	//
	// When nil a default logger will be used that logs to os.Stderr at ERROR
	// level using JSON format.
	//
	// Note: Logs from librdkafka are logged at DEBUG level. If the provided
	// logger is configured at DEBUG level, you will see logs from librdkafka
	// as well as Kefka logs.
	//
	// Applies To: Consumer, Producer
	Logger Logger

	// A callback that is called when Confluent Kafka Client/librdkafka returns
	// a Kafka error. This can be useful for logging errors or capturing metrics.
	// The default value is nil and won't be called.
	//
	// Applies To: Consumer, Producer
	OnError func(error)
}

func (c *KafkaConfig) init() {
	if c.SessionTimeout == 0 {
		c.SessionTimeout = defaultSessionTimeout
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = defaultHeartbeatInterval
	}
	if c.AutoOffsetReset == "" {
		c.AutoOffsetReset = defaultOffsetReset
	}
	if c.MessageMaxBytes == 0 {
		c.MessageMaxBytes = defaultMessageMaxBytes
	}
	if c.MaxFetchBytes == 0 {
		c.MaxFetchBytes = defaultMaxFetchBytes
	}
	if c.CommitInterval == 0 {
		c.CommitInterval = defaultCommitInterval
	}
	if c.RequiredAcks == "" {
		c.RequiredAcks = defaultRequiredAck
	}
	if c.SecurityProtocol == "" {
		c.SecurityProtocol = defaultSecurityProtocol
	}
	if c.PollTimeout == 0 {
		c.PollTimeout = defaultPollTimeout
	}
	if c.AcknowledgmentStrategy == "" {
		c.AcknowledgmentStrategy = defaultAcknowledgmentStrategy
	}
	if c.SASLMechanism == "" {
		c.SASLMechanism = defaultSASLMechanism
	}
	if c.OnError == nil {
		c.OnError = func(err error) {}
	}
}

// ConfigFromEnv loads the configuration for the Kafka client from the environment.
func ConfigFromEnv() (*KafkaConfig, error) {
	var config KafkaConfig
	err := envconfig.Process(context.Background(), &config)
	if err != nil {
		return nil, err
	}

	config.Logger = NewNopLogger()
	config.OnError = func(err error) {}

	return &config, nil
}
