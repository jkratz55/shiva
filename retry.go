package shiva

import (
	"errors"
	"fmt"
	"sync"
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
		maxAttempts:         defaultMaxRetries,
		initialDelay:        defaultInitialDelay,
		maxDelay:            defaultMaxDelay,
		retryableErrorsOnly: false,
		onError:             func(err error) {},
	}
}

// RetryOption defines a functional option type used to configure retry behavior for the Retry
// mechanism.
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

// WithMaxDelay sets the max delay between retries effectively capping the max time to delay between
// attempts.
func WithMaxDelay(maxDelay time.Duration) RetryOption {
	return func(config *retryConfig) {
		config.maxDelay = maxDelay
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
// to recover for transient errors. The default initial delay is 500ms with a max delay of 10s.
//
// By default, all errors will be retried. However, using the WithRetryableErrorsOnly option the
// middleware can be configured to only retry errors marked as retryable which can be achieved by
// wrapping error that should be retried with the WrapAsRetryable func.
//
// Retry will return all errors if the max attempts is exhausted without success, but once the
// operation succeeds the previous errors are discard. It is recommended to either handle logging
// and instrumentation within the Handler implementation or utilize the WithOnError RetryOption
// which is a callback invoked whenever an error is returned from the Handler.
//
// A panic will occur if next is nil.
func Retry(next Handler, opts ...RetryOption) Middleware {
	if next == nil {
		panic("cannot wrap nil Handler, next cannot be nil")
	}

	conf := newRetryConfig()
	for _, opt := range opts {
		opt(conf)
	}

	return func(next Handler) Handler {
		return &RetryHandler{
			next: next,
			conf: conf,
			backoffPool: sync.Pool{
				New: func() interface{} {
					bo := backoff.NewExponentialBackOff()
					bo.InitialInterval = conf.initialDelay
					bo.MaxInterval = conf.maxDelay
					bo.Multiplier = 2
					return bo
				},
			},
		}
	}
}

// RetryHandler is a middleware that wraps a Handler to provide retry logic with configurable behavior
// and backoff.
type RetryHandler struct {
	next        Handler
	conf        *retryConfig
	backoffPool sync.Pool
}

func (r *RetryHandler) Handle(msg Message) error {
	var err error

	bo := r.backoffPool.Get().(*backoff.ExponentialBackOff)
	bo.Reset() // Do not remove this line or the instance of ExponentialBackOff will be in an unpredictable state
	defer r.backoffPool.Put(bo)

	for i := 0; i < r.conf.maxAttempts; i++ {
		handlerErr := r.next.Handle(msg)
		if handlerErr == nil {
			// If the next Handler returns a nil error value it is assumed the operation succeeded
			return nil
		}

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
			dur := bo.NextBackOff()

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
