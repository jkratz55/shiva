package shiva

import (
	"fmt"
	"strings"
)

type SaslMechanism string

const (
	Plain       SaslMechanism = "PLAIN"
	ScramSha256 SaslMechanism = "SCRAM-SHA-256"
	ScramSha512 SaslMechanism = "SCRAM-SHA-512"
)

func (sm *SaslMechanism) UnmarshalText(b []byte) error {
	s, err := ParseSaslMechanism(string(b))
	if err != nil {
		return err
	}
	*sm = s
	return nil
}

func (sm *SaslMechanism) String() string {
	return string(*sm)
}

func ParseSaslMechanism(s string) (SaslMechanism, error) {
	switch strings.ToUpper(s) {
	case "PLAIN":
		return Plain, nil
	case "SCRAM-SHA-256":
		return ScramSha256, nil
	case "SCRAM-SHA-512":
		return ScramSha512, nil
	default:
		return "", fmt.Errorf("kafka:invalid/unsupported sasl mechanism: %s", s)
	}
}
