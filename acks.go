package shiva

import (
	"fmt"
	"strings"
)

type RequiredAck string

const (
	// AckNone disables acknowledgements from the brokers. The producer will not
	// wait for any acknowledgement from the broker and the broker does not wait
	// for the message to be written before it responds.
	AckNone RequiredAck = "none"

	// AckLeader ensures the leader broker must receive the record and successfully
	// write it to its local log before responding.
	AckLeader RequiredAck = "leader"

	// AckAll ensures the leader and all in-sync replicas must receive the record
	// and successfully write it to their local log before responding.
	AckAll RequiredAck = "all"
)

func (a *RequiredAck) UnmarshalText(text []byte) error {
	ack, err := ParseRequiredAck(string(text))
	if err != nil {
		return err
	}
	*a = ack
	return nil
}

func (a *RequiredAck) value() int {
	switch *a {
	case AckNone:
		return 0
	case AckLeader:
		return 1
	case AckAll:
		return -1
	default:
		panic(fmt.Sprintf("kafka: unknown ack value: %s", *a))
	}
}

func ParseRequiredAck(s string) (RequiredAck, error) {
	switch strings.ToLower(s) {
	case "none":
		return AckNone, nil
	case "leader":
		return AckLeader, nil
	case "all":
		return AckAll, nil
	default:
		return "", fmt.Errorf("kafka: invalid ack value: %s", s)
	}
}
