package shiva

import (
	"fmt"
	"strings"
)

type SecurityProtocol string

const (
	Plaintext     SecurityProtocol = "plaintext"
	Ssl           SecurityProtocol = "ssl"
	SaslPlaintext SecurityProtocol = "sasl_plaintext"
	SaslSsl       SecurityProtocol = "sasl_ssl"
)

func (sp *SecurityProtocol) UnmarshalText(b []byte) error {
	s, err := ParseSecurityProtocol(string(b))
	if err != nil {
		return err
	}
	*sp = s
	return nil
}

func (sp *SecurityProtocol) String() string {
	return string(*sp)
}

func ParseSecurityProtocol(s string) (SecurityProtocol, error) {
	switch strings.ToLower(s) {
	case "plaintext":
		return Plaintext, nil
	case "ssl":
		return Ssl, nil
	case "sasl_plaintext":
		return SaslPlaintext, nil
	case "sasl_ssl":
		return SaslSsl, nil
	default:
		return "", fmt.Errorf("kafka: invalid security protocol: %s", s)
	}
}
