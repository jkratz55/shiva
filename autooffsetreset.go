package shiva

import (
	"fmt"
	"strings"
)

type AutoOffsetReset string

const (
	Earliest AutoOffsetReset = "earliest"
	Latest   AutoOffsetReset = "latest"
)

func (a *AutoOffsetReset) UnmarshalText(text []byte) error {
	val, err := ParseAutoOffsetReset(string(text))
	if err != nil {
		return err
	}
	*a = val
	return nil
}

func (a *AutoOffsetReset) String() string {
	return string(*a)
}

func ParseAutoOffsetReset(s string) (AutoOffsetReset, error) {
	switch strings.ToLower(s) {
	case "earliest":
		return Earliest, nil
	case "latest":
		return Latest, nil
	default:
		return "", fmt.Errorf("kafka: invalid auto offset reset: %s", s)
	}
}
