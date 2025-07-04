package shiva

import (
	"context"

	"github.com/stretchr/testify/mock"
)

type mockHandler struct {
	mock.Mock

	attempts int
}

func (m *mockHandler) Handle(ctx context.Context, msg Message) error {
	m.attempts++
	args := m.Called(ctx, msg)
	if args.Get(0) != nil {
		return args.Error(0)
	}
	return nil
}

type mockDeadLetterHandler struct {
	mock.Mock

	called int
}

func (m *mockDeadLetterHandler) Handle(ctx context.Context, msg Message, err error) {
	m.called++
	_ = m.Called(ctx, msg, err)
}
