package shiva

import (
	"errors"
)

type retryable interface {
	IsRetryable() bool
}

type RetryableError struct {
	err       error
	retryable bool
}

func (r RetryableError) Error() string {
	return r.err.Error()
}

func (r RetryableError) IsRetryable() bool {
	return r.retryable
}

func (r RetryableError) Unwrap() error {
	return r.err
}

// IsRetryable determines if the given error can and/or should be retried.
func IsRetryable(err error) bool {
	for err != nil {
		if re, ok := err.(retryable); ok && re.IsRetryable() {
			return true
		}
		err = errors.Unwrap(err)
	}
	return false
}

// WrapErrorAsRetryable wraps an error in a RetryableError, marking it as retryable.
func WrapErrorAsRetryable(e error) error {
	if e == nil {
		return nil
	}
	return RetryableError{
		err:       e,
		retryable: true,
	}
}
