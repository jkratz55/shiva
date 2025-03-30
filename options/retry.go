package options

import (
	"time"
)

type RetryOpts struct {
	// Maximum number of attempts before giving up and returning an error
	MaxAttempts int

	// Initial delay before retrying
	InitialDelay time.Duration

	// Flag to control if message processing should only be retried if the error is known to be a
	// RetryableError
	RetryableErrorsOnly bool

	// Callback that is invoked when an error occurs. This can be useful for logging and
	// instrumentation
	OnError func(err error)
}

func (o *RetryOpts) SetMaxAttempts(maxAttempts int) {
	o.MaxAttempts = maxAttempts
}

func (o *RetryOpts) SetInitialDelay(initialDelay time.Duration) {
	o.InitialDelay = initialDelay
}

func (o *RetryOpts) SetRetryableErrorsOnly(retryableErrorsOnly bool) {
	o.RetryableErrorsOnly = retryableErrorsOnly
}

func (o *RetryOpts) SetOnError(onError func(err error)) {
	o.OnError = onError
}

func Retry() *RetryOpts {
	return &RetryOpts{
		MaxAttempts:         5,
		InitialDelay:        1 * time.Second,
		RetryableErrorsOnly: false,
		OnError:             func(err error) {},
	}
}
