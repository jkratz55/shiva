package shiva

import (
	"errors"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v5"
)

type retryConfig struct {
	maxAttempts         int
	initialDelay        time.Duration
	maxDelay            time.Duration
	retryableErrorsOnly bool
	onError             func(err error)
}

func newRetryConfig() *retryConfig {
	return &retryConfig{
		maxAttempts:         5,
		initialDelay:        500 * time.Millisecond,
		maxDelay:            10 * time.Second,
		retryableErrorsOnly: false,
		onError:             func(err error) {},
	}
}

type RetryOption func(*retryConfig)

// WithMaxAttempts sets the maximum number of attempts for the operation before giving up.
//
// Panics if maxAttempts is less than 1.
func WithMaxAttempts(maxAttempts int) RetryOption {
	if maxAttempts < 1 {
		panic("maxAttempts must be greater than or equal to 1")
	}
	return func(config *retryConfig) {
		config.maxAttempts = maxAttempts
	}
}

// WithInitialDelay sets the initial delay before the first retry.
func WithInitialDelay(initialDelay time.Duration) RetryOption {
	return func(config *retryConfig) {
		config.initialDelay = initialDelay
	}
}

// WithRetryableErrorsOnly restricts retries to only occur for retryable errors when set to true.
func WithRetryableErrorsOnly(retryableErrorsOnly bool) RetryOption {
	return func(config *retryConfig) {
		config.retryableErrorsOnly = retryableErrorsOnly
	}
}

// WithOnError sets a callback function that gets invoked when the next Handler in the chain returns
// an error. Passing nil is a no-op.
func WithOnError(onError func(err error)) RetryOption {
	return func(config *retryConfig) {
		if onError == nil {
			// no-op
			return
		}
		config.onError = onError
	}
}

// Retry is a middleware for a Handler that will retry processing messages when the Handler
// implementation returns an error up to the max attempts. By default, Retry will make at most
// five attempts but that can be configured by passing one or many RetryOption. An exponential
// backoff is used between attempts starting with the initial delay to give the system a chance
// to recover for transient errors.
//
// By default, all errors will be retried. However, using the WithRetryableErrorsOnly option the
// middleware can be configured to only retry errors marked as retryable which can be achieved by
// wrapping error that should be retried with the WrapErrorAsRetryable func.
//
// Retry will return all errors if the max attempts is exhausted without success, but once the
// operation succeeds the previous errors are discard. It is recommended to either handle logging
// and instrumentation within the Handler implementation or utilize the WithOnError RetryOption
// which is a callback invoked whenever an error is returned from the Handler.
//
// A panic will occur if next is nil.
func Retry(next Handler, opts ...RetryOption) Handler {
	if next == nil {
		panic("cannot wrap nil Handler, next cannot be nil")
	}

	conf := newRetryConfig()
	for _, opt := range opts {
		opt(conf)
	}

	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = conf.initialDelay
	bo.MaxInterval = 2 * time.Minute
	bo.Multiplier = 2

	return &RetryHandler{
		next:    next,
		conf:    conf,
		backoff: bo,
	}
}

type RetryHandler struct {
	next    Handler
	conf    *retryConfig
	backoff *backoff.ExponentialBackOff
}

func (r *RetryHandler) Handle(msg any) error {
	var err error

	for i := 0; i < r.conf.maxAttempts; i++ {
		handlerErr := r.next.Handle(msg)
		if handlerErr == nil {
			// If the next Handler returns a nil error value it is assumed the operation succeeded
			return nil
		}

		r.backoff.Reset()
		err = errors.Join(err, handlerErr)

		if r.conf.onError != nil {
			r.conf.onError(handlerErr)
		}

		// If configured to only retry when an error is known to be retryable return early if the
		// error returned from the Handler is not retryable.
		if r.conf.retryableErrorsOnly && !IsRetryable(handlerErr) {
			return handlerErr
		}

		// We only want to delay if there is at least one more attempt
		if i < r.conf.maxAttempts-1 {
			dur := r.backoff.NextBackOff()

			// If the max delay has been exceeded on ExponentialBackOff then we'll simply delay
			// for the maximum configured value. Otherwise, delay for the duration returned from
			// ExponentialBackOff
			if dur == backoff.Stop {
				time.Sleep(r.conf.maxDelay)
			} else {
				time.Sleep(dur)
			}
		}
	}

	// If we reach this point all attempts were exhausted
	return fmt.Errorf("max attempts processing message: %w", err)
}
