package shiva

import (
	"time"
)

// ------------------------------------------------------------------------------------------------
// Defines defaults and constants
//
// If these are changed the documentation on the Config struct needs to updated as well to reflect
// the default behavior.
// ------------------------------------------------------------------------------------------------

const (
	// --------------------------------------------------------------------------------------------
	// Consumer defaults
	// --------------------------------------------------------------------------------------------

	defaultCommitInterval         = 5 * time.Second
	defaultHeartbeatInterval      = 3 * time.Second
	defaultSessionTimeout         = 45 * time.Second
	defaultPollTimeout            = 100 * time.Millisecond
	defaultOffsetReset            = Earliest
	defaultAcknowledgmentStrategy = AcknowledgmentStrategyPostProcessing
	defaultMaxFetchBytes          = 52428800 // 50MB

	// --------------------------------------------------------------------------------------------
	// Producer defaults
	// --------------------------------------------------------------------------------------------

	defaultRequiredAck = AckLeader

	// --------------------------------------------------------------------------------------------
	// Common defaults
	// --------------------------------------------------------------------------------------------

	defaultMessageMaxBytes  = 1048576 // 1MB
	defaultSecurityProtocol = Plaintext
	defaultSASLMechanism    = Plain

	// --------------------------------------------------------------------------------------------
	// Retry defaults
	// --------------------------------------------------------------------------------------------

	defaultMaxRetries   = 5
	defaultInitialDelay = 500 * time.Millisecond
	defaultMaxDelay     = 10 * time.Second
)
