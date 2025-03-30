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
	defaultCommitInterval    = 5 * time.Second
	defaultHeartbeatInterval = 5 * time.Second
	defaultSessionTimeout    = 45 * time.Second
)
