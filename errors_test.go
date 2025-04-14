package shiva

import (
	"errors"
	"fmt"
	"testing"
)

func TestIsRetryable(t *testing.T) {

	originalErr := errors.New("original error")
	re := WrapAsRetryable(originalErr)

	err := fmt.Errorf("making http call: %w", re)
	err = fmt.Errorf("service layer fun: %w", err)

	if IsRetryable(err) {
		fmt.Println("I should retry")
	} else {
		fmt.Println("I should not retry")
	}
}
