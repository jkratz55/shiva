package shiva

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

// todo: unit tests

func TestRetryHandler_Handle(t *testing.T) {

	type test struct {
		name      string
		handler   Handler
		opts      []RetryOption
		shouldErr bool
	}

	tests := []test{
		{
			name:      "Pass First Attempt",
			handler:   nil,
			opts:      nil,
			shouldErr: false,
		},
		{
			name:      "Fail All Attempts",
			handler:   nil,
			opts:      nil,
			shouldErr: true,
		},
		{
			name:      "Fail First Attempt and Pass Second Attempt",
			handler:   nil,
			opts:      nil,
			shouldErr: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			retryHandler := Retry(test.handler, test.opts...)
			err := retryHandler.Handle(context.Background(), Message{})
			if test.shouldErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
