package shiva

import (
	"fmt"
	"strings"
)

// AcknowledgmentStrategy defines the strategy for acknowledging messages.
type AcknowledgmentStrategy string

// UnmarshalText converts the provided text bytes into an AcknowledgmentStrategy value or returns
// an error if invalid.
func (a *AcknowledgmentStrategy) UnmarshalText(text []byte) error {
	strategy, err := ParseAcknowledgmentStrategy(string(text))
	if err != nil {
		return err
	}
	*a = strategy
	return nil
}

func (a *AcknowledgmentStrategy) String() string {
	return string(*a)
}

const (
	// AcknowledgmentStrategyNone indicates messages are not acknowledged.
	AcknowledgmentStrategyNone AcknowledgmentStrategy = "none"

	// AcknowledgmentStrategyPreProcessing indicates messages are acknowledged after they've been
	// polled from Kafka but before the message is passed to the Handler to be processed. This is
	// often referred to as `at most once` delivery.
	AcknowledgmentStrategyPreProcessing AcknowledgmentStrategy = "pre-processing"

	// AcknowledgmentStrategyPostProcessing indicates messages are acknowledged only after the Handler
	// returns. This is often referred to as `at least once` delivery.
	AcknowledgmentStrategyPostProcessing AcknowledgmentStrategy = "post-processing"
)

// ParseAcknowledgmentStrategy parses a string into an AcknowledgmentStrategy or returns an error if
// unsupported.
func ParseAcknowledgmentStrategy(strategy string) (AcknowledgmentStrategy, error) {
	if strings.TrimSpace(strategy) == "" {
		return AcknowledgmentStrategyNone, fmt.Errorf("acknowledgment strategy empty/blank")
	}

	switch strings.ToLower(strategy) {
	case string(AcknowledgmentStrategyNone):
		return AcknowledgmentStrategyNone, nil
	case string(AcknowledgmentStrategyPreProcessing):
		return AcknowledgmentStrategyPreProcessing, nil
	case string(AcknowledgmentStrategyPostProcessing):
		return AcknowledgmentStrategyPostProcessing, nil
	default:
		return AcknowledgmentStrategyNone, fmt.Errorf("unsupported acknowledgment strategy: %s", strategy)
	}
}
