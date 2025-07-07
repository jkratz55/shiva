package shiva

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRetryHandler_Handle(t *testing.T) {

	msg := Message{
		Topic:     "test-topic",
		Partition: 0,
		Offset:    1001,
		Key:       []byte("hello"),
		Value:     []byte("world"),
		Timestamp: time.Now(),
	}

	type test struct {
		name             string
		handlerFactory   func() Handler
		opts             []RetryOption
		shouldErr        bool
		expectedAttempts int
		onErrorCalled    int
	}

	onErrorCalledCount := 0

	tests := []test{
		{
			name: "Pass First Attempt",
			handlerFactory: func() Handler {
				h := new(mockHandler)
				h.On("Handle", context.Background(), msg).Return(nil)
				return h
			},
			opts:             nil,
			shouldErr:        false,
			expectedAttempts: 1,
		},
		{
			name: "Fail All Attempts",
			handlerFactory: func() Handler {
				h := new(mockHandler)
				h.On("Handle", context.Background(), msg).Return(assert.AnError)
				return h
			},
			opts:             []RetryOption{WithMaxAttempts(3)},
			shouldErr:        true,
			expectedAttempts: 3,
		},
		{
			name: "Fail First Attempt and Pass Next Attempt",
			handlerFactory: func() Handler {
				h := new(mockHandler)
				h.On("Handle", context.Background(), msg).Return(assert.AnError).Once()
				h.On("Handle", context.Background(), msg).Return(nil)
				return h
			},
			opts:             []RetryOption{WithMaxAttempts(3)},
			shouldErr:        false,
			expectedAttempts: 2,
		},
		{
			name: "Only Retry Retryable Errors and Returned Error is Not Retryable",
			handlerFactory: func() Handler {
				h := new(mockHandler)
				h.On("Handle", context.Background(), msg).Return(assert.AnError)
				return h
			},
			opts:             []RetryOption{WithMaxAttempts(3), WithRetryableErrorsOnly(true)},
			shouldErr:        true,
			expectedAttempts: 1,
		},
		{
			name: "Only Retry Retryable Errors with First Error Being Retryable and Second Error is Not",
			handlerFactory: func() Handler {
				h := new(mockHandler)
				h.On("Handle", context.Background(), msg).Return(WrapAsRetryable(assert.AnError)).Once()
				h.On("Handle", context.Background(), msg).Return(assert.AnError)
				return h
			},
			opts:             []RetryOption{WithMaxAttempts(3), WithRetryableErrorsOnly(true)},
			shouldErr:        true,
			expectedAttempts: 2,
		},
		{
			name: "All Attempts Fail with OnError callback",
			handlerFactory: func() Handler {
				h := new(mockHandler)
				h.On("Handle", context.Background(), msg).Return(assert.AnError)
				return h
			},
			opts: []RetryOption{WithMaxAttempts(3), WithOnError(func(err error) {
				onErrorCalledCount++
			})},
			shouldErr:        true,
			expectedAttempts: 3,
			onErrorCalled:    3,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			onErrorCalledCount = 0
			handler := test.handlerFactory()
			retryHandler := Retry(handler, test.opts...)
			err := retryHandler.Handle(context.Background(), msg)
			assert.Equal(t, test.shouldErr, err != nil)
			assert.Equal(t, test.expectedAttempts, handler.(*mockHandler).attempts)
			assert.Equal(t, test.onErrorCalled, onErrorCalledCount)
		})
	}
}
