package options

import (
	"time"
)

type ConsumerOptions struct {
	BootstrapServers []string
	GroupID          string
	SessionTimeout   time.Duration
	HeartbeatTimeout time.Duration
	CommitInterval   time.Duration
	PollTimeout      time.Duration
	// AutoOffsetReset              AutoOffsetReset
	MessageMaxBytes int
	// SecurityProtocol             SecurityProtocol
	CertificateAuthorityLocation string
	CertificateLocation          string
	CertificateKeyLocation       string
	CertificateKeyPassword       string
	SkipTlsVerification          bool
	// SASLMechanism                SaslMechanism
	SASLUsername string
	SASLPassword string
	// Logger                       *slog.Logger
	OnError func(error)
}

func Consumer() *ConsumerOptions {
	return &ConsumerOptions{}
}

func (c *ConsumerOptions) SetBootstrapServers(servers []string) *ConsumerOptions {
	c.BootstrapServers = servers
	return c
}
