package shiva

import (
	"context"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/testcontainers/testcontainers-go"
	kafkatest "github.com/testcontainers/testcontainers-go/modules/kafka"

	"github.com/jkratz55/shiva/internal"
)

func TestNewConsumer(t *testing.T) {

	type testCase struct {
		name      string
		config    KafkaConfig
		topic     string
		handler   Handler
		shouldErr bool
	}

	tests := []testCase{
		{
			name: "Valid Configuration",
			config: KafkaConfig{
				BootstrapServers:       []string{"localhost:9092"},
				GroupID:                "test-group",
				AutoOffsetReset:        Earliest,
				AcknowledgmentStrategy: AcknowledgmentStrategyPostProcessing,
			},
			topic:     "test",
			handler:   new(mockHandler),
			shouldErr: false,
		},
		{
			name: "Invalid Configuration - Missing Group ID",
			config: KafkaConfig{
				BootstrapServers:       []string{"localhost:9092"},
				AutoOffsetReset:        Earliest,
				AcknowledgmentStrategy: AcknowledgmentStrategyPostProcessing,
			},
			topic:     "test",
			handler:   new(mockHandler),
			shouldErr: true,
		},
		{
			name: "Invalid Configuration - Nil Handler",
			config: KafkaConfig{
				BootstrapServers:       []string{"localhost:9092"},
				GroupID:                "test-group",
				AutoOffsetReset:        Earliest,
				AcknowledgmentStrategy: AcknowledgmentStrategyPostProcessing,
			},
			topic:     "test",
			handler:   nil,
			shouldErr: true,
		},
		{
			name: "Invalid Configuration - Empty Topic",
			config: KafkaConfig{
				BootstrapServers:       []string{"localhost:9092"},
				GroupID:                "test-group",
				AutoOffsetReset:        Earliest,
				AcknowledgmentStrategy: AcknowledgmentStrategyPostProcessing,
			},
			topic:     "",
			handler:   new(mockHandler),
			shouldErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			consumer, err := NewConsumer(test.config, test.topic, test.handler)
			assert.Equal(t, test.shouldErr, err != nil)
			if !test.shouldErr {
				test.config.init()
				assert.Equal(t, &test.config, consumer.config)
				assert.Equal(t, test.topic, consumer.topic)
				assert.Equal(t, test.handler, consumer.handler)
			}
		})
	}
}

func TestConsumer_Assignment(t *testing.T) {

	type testCase struct {
		name        string
		init        func() *Consumer
		expected    TopicPartitions
		expectedErr bool
	}

	config := KafkaConfig{
		BootstrapServers: []string{"localhost:9092"},
		GroupID:          "test-group",
	}

	tests := []testCase{
		{
			name: "Return Valid Assignments",
			init: func() *Consumer {
				base := new(mockKafkaConsumer)
				base.On("Assignment").Return([]kafka.TopicPartition{
					{
						Topic:     StringPtr("test"),
						Partition: 0,
						Offset:    0,
					},
					{
						Topic:     StringPtr("test"),
						Partition: 1,
						Offset:    0,
					},
					{
						Topic:     StringPtr("test"),
						Partition: 2,
						Offset:    0,
					},
				}, nil)
				consumer, err := NewConsumer(config, "test", new(mockHandler))
				assert.NoError(t, err)
				consumer.baseConsumer = base
				return consumer
			},
			expected: TopicPartitions{
				{
					Topic:     "test",
					Partition: 0,
					Offset:    0,
				},
				{
					Topic:     "test",
					Partition: 1,
					Offset:    0,
				},
				{
					Topic:     "test",
					Partition: 2,
					Offset:    0,
				},
			},
			expectedErr: false,
		},
		{
			name: "Kafka Client Returns Error",
			init: func() *Consumer {
				base := new(mockKafkaConsumer)
				base.On("Assignment").Return([]kafka.TopicPartition{}, assert.AnError)
				consumer, err := NewConsumer(config, "test", new(mockHandler))
				assert.NoError(t, err)
				consumer.baseConsumer = base
				return consumer
			},
			expected:    nil,
			expectedErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			consumer := test.init()
			assignments, err := consumer.Assignment()
			assert.Equal(t, test.expected, assignments)
			assert.Equal(t, test.expectedErr, err != nil)
		})
	}
}

func TestConsumer_Subscription(t *testing.T) {

	type testCase struct {
		name        string
		init        func() *Consumer
		expected    []string
		expectedErr bool
	}

	config := KafkaConfig{
		BootstrapServers: []string{"localhost:9092"},
		GroupID:          "test-group",
	}

	tests := []testCase{
		{
			name: "Client Returns Subscriptions",
			init: func() *Consumer {
				base := new(mockKafkaConsumer)
				base.On("Subscription").Return([]string{"test"}, nil)
				consumer, err := NewConsumer(config, "test", new(mockHandler))
				assert.NoError(t, err)
				consumer.baseConsumer = base
				return consumer
			},
			expected:    []string{"test"},
			expectedErr: false,
		},
		{
			name: "Client Returns Error",
			init: func() *Consumer {
				base := new(mockKafkaConsumer)
				base.On("Subscription").Return([]string{}, assert.AnError)
				consumer, err := NewConsumer(config, "test", new(mockHandler))
				assert.NoError(t, err)
				consumer.baseConsumer = base
				return consumer
			},
			expected:    []string{},
			expectedErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			consumer := test.init()
			subscriptions, err := consumer.Subscription()
			assert.Equal(t, test.expected, subscriptions)
			assert.Equal(t, test.expectedErr, err != nil)
		})
	}
}

func TestConsumer_Position(t *testing.T) {

}

func TestConsumer_Commit(t *testing.T) {

}

func TestConsumer_Committed(t *testing.T) {

}

func TestConsumer_Pause(t *testing.T) {

}

func TestConsumer_Resume(t *testing.T) {

}

func TestConsumer_Lag(t *testing.T) {

	config := KafkaConfig{
		BootstrapServers: []string{"localhost:9092"},
		GroupID:          "test-group",
	}

	type testCase struct {
		name        string
		init        func() *Consumer
		expected    map[string]int64
		expectedErr bool
	}

	tests := []testCase{
		{
			name: "Success",
			init: func() *Consumer {
				base := new(mockKafkaConsumer)
				base.On("Assignment").Return([]kafka.TopicPartition{
					{
						Topic:     StringPtr("test"),
						Partition: 0,
						Offset:    0,
					},
					{
						Topic:     StringPtr("test"),
						Partition: 1,
						Offset:    0,
					},
				}, nil)
				base.On("Position", mock.Anything).Return([]kafka.TopicPartition{
					{
						Topic:     StringPtr("test"),
						Partition: 0,
						Offset:    100,
					},
					{
						Topic:     StringPtr("test"),
						Partition: 1,
						Offset:    200,
					},
				}, nil)
				base.On("QueryWatermarkOffsets", mock.Anything, mock.Anything, mock.Anything).Return(int64(0), int64(300), nil).Once()
				base.On("QueryWatermarkOffsets", mock.Anything, mock.Anything, mock.Anything).Return(int64(0), int64(500), nil).Once()

				consumer, err := NewConsumer(config, "test", new(mockHandler))
				assert.NoError(t, err)
				consumer.baseConsumer = base
				return consumer
			},
			expected: map[string]int64{
				"test|0": 200,
				"test|1": 300,
			},
			expectedErr: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			consumer := test.init()
			lag, err := consumer.Lag()
			assert.Equal(t, test.expected, lag)
			assert.Equal(t, test.expectedErr, err != nil)
		})
	}
}

func TestConsumer_GetWaterMarkOffset(t *testing.T) {

	config := KafkaConfig{
		BootstrapServers: []string{"localhost:9092"},
		GroupID:          "test-group",
	}

	type testCase struct {
		name        string
		init        func() *Consumer
		expected    map[string]Watermark
		expectedErr bool
	}

	tests := []testCase{
		{
			name: "Success",
			init: func() *Consumer {
				base := new(mockKafkaConsumer)
				base.On("Assignment").Return([]kafka.TopicPartition{
					{
						Topic:     StringPtr("test"),
						Partition: 0,
						Offset:    0,
					},
					{
						Topic:     StringPtr("test"),
						Partition: 1,
						Offset:    0,
					},
				}, nil)
				base.On("GetWatermarkOffsets", mock.Anything, mock.Anything).Return(int64(1111), int64(2222), nil).Once()
				base.On("GetWatermarkOffsets", mock.Anything, mock.Anything).Return(int64(3333), int64(4444), nil).Once()

				consumer, err := NewConsumer(config, "test", new(mockHandler))
				assert.NoError(t, err)
				consumer.baseConsumer = base
				return consumer
			},
			expected: map[string]Watermark{
				"test|0": {
					Low:  1111,
					High: 2222,
				},
				"test|1": {
					Low:  3333,
					High: 4444,
				},
			},
			expectedErr: false,
		},
		{
			name: "Error Getting Assignments",
			init: func() *Consumer {
				base := new(mockKafkaConsumer)
				base.On("Assignment").Return([]kafka.TopicPartition{}, assert.AnError)
				consumer, err := NewConsumer(config, "test", new(mockHandler))
				assert.NoError(t, err)
				consumer.baseConsumer = base
				return consumer
			},
			expected:    nil,
			expectedErr: true,
		},
		{
			name: "Error Getting Watermark Offsets",
			init: func() *Consumer {
				base := new(mockKafkaConsumer)
				base.On("Assignment").Return([]kafka.TopicPartition{
					{
						Topic:     StringPtr("test"),
						Partition: 0,
						Offset:    0,
					},
				}, nil)
				base.On("GetWatermarkOffsets", mock.Anything, mock.Anything).Return(int64(0), int64(0), assert.AnError).Once()

				consumer, err := NewConsumer(config, "test", new(mockHandler))
				assert.NoError(t, err)
				consumer.baseConsumer = base
				return consumer
			},
			expected:    nil,
			expectedErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			consumer := test.init()
			watermarks, err := consumer.GetWatermarkOffsets()
			assert.Equal(t, test.expected, watermarks)
			assert.Equal(t, test.expectedErr, err != nil)
		})
	}
}

func TestConsumer_QueryWatermarkOffsets(t *testing.T) {
	config := KafkaConfig{
		BootstrapServers: []string{"localhost:9092"},
		GroupID:          "test-group",
	}

	type testCase struct {
		name        string
		init        func() *Consumer
		expected    map[string]Watermark
		expectedErr bool
	}

	tests := []testCase{
		{
			name: "Success",
			init: func() *Consumer {
				base := new(mockKafkaConsumer)
				base.On("Assignment").Return([]kafka.TopicPartition{
					{
						Topic:     StringPtr("test"),
						Partition: 0,
						Offset:    0,
					},
					{
						Topic:     StringPtr("test"),
						Partition: 1,
						Offset:    0,
					},
				}, nil)
				base.On("QueryWatermarkOffsets", mock.Anything, mock.Anything, mock.Anything).Return(int64(1111), int64(2222), nil).Once()
				base.On("QueryWatermarkOffsets", mock.Anything, mock.Anything, mock.Anything).Return(int64(3333), int64(4444), nil).Once()

				consumer, err := NewConsumer(config, "test", new(mockHandler))
				assert.NoError(t, err)
				consumer.baseConsumer = base
				return consumer
			},
			expected: map[string]Watermark{
				"test|0": {
					Low:  1111,
					High: 2222,
				},
				"test|1": {
					Low:  3333,
					High: 4444,
				},
			},
			expectedErr: false,
		},
		{
			name: "Error Getting Assignments",
			init: func() *Consumer {
				base := new(mockKafkaConsumer)
				base.On("Assignment").Return([]kafka.TopicPartition{}, assert.AnError)
				consumer, err := NewConsumer(config, "test", new(mockHandler))
				assert.NoError(t, err)
				consumer.baseConsumer = base
				return consumer
			},
			expected:    nil,
			expectedErr: true,
		},
		{
			name: "Error Querying Watermark Offsets",
			init: func() *Consumer {
				base := new(mockKafkaConsumer)
				base.On("Assignment").Return([]kafka.TopicPartition{
					{
						Topic:     StringPtr("test"),
						Partition: 0,
						Offset:    0,
					},
				}, nil)
				base.On("QueryWatermarkOffsets", mock.Anything, mock.Anything, mock.Anything).Return(int64(0), int64(0), assert.AnError).Once()

				consumer, err := NewConsumer(config, "test", new(mockHandler))
				assert.NoError(t, err)
				consumer.baseConsumer = base
				return consumer
			},
			expected:    nil,
			expectedErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			consumer := test.init()
			watermarks, err := consumer.QueryWatermarkOffsets(context.Background())
			assert.Equal(t, test.expected, watermarks)
			assert.Equal(t, test.expectedErr, err != nil)
		})
	}
}

func TestConsumer_IsRunning(t *testing.T) {
	consumer := new(Consumer)
	assert.False(t, consumer.IsRunning())
	consumer.running = true
	assert.True(t, consumer.IsRunning())
}

func TestConsumer_IsClosed(t *testing.T) {
	config := KafkaConfig{
		BootstrapServers: []string{"localhost:9092"},
		GroupID:          "test-group",
	}

	base := new(mockKafkaConsumer)
	base.On("IsClosed").Return(false).Once()
	base.On("IsClosed").Return(true).Once()

	consumer, err := NewConsumer(config, "test", new(mockHandler))
	assert.NoError(t, err)
	consumer.baseConsumer = base

	assert.False(t, consumer.IsClosed())
	consumer.Close()
	assert.True(t, consumer.IsClosed())
}

func TestConsumer_Close(t *testing.T) {
	config := KafkaConfig{
		BootstrapServers: []string{"localhost:9092"},
		GroupID:          "test-group",
	}

	base := new(mockKafkaConsumer)
	base.On("Poll", mock.Anything).Return(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     StringPtr("test"),
			Partition: 0,
			Offset:    100,
		},
		Value: []byte("Hello World!"),
		Key:   []byte("hello"),
	})
	base.On("IsClosed").Return(false)
	base.On("StoreMessage", mock.Anything).Return([]kafka.TopicPartition{}, nil)
	base.On("Commit").Return([]kafka.TopicPartition{}, nil)
	base.On("Close").Return(nil)

	consumer, err := NewConsumer(config, "test", HandlerFunc(func(ctx context.Context, msg Message) error {
		return nil
	}))
	assert.NoError(t, err)
	consumer.baseConsumer = base

	assert.False(t, consumer.running)

	go func() {
		err := consumer.Run()
		assert.NoError(t, err)
	}()

	time.Sleep(2 * time.Second)
	assert.True(t, consumer.running)
	consumer.Close()
	time.Sleep(2 * time.Second)

	assert.False(t, consumer.running)
}

func TestConsumer_Run(t *testing.T) {

	config := KafkaConfig{
		BootstrapServers: []string{"localhost:9092"},
		GroupID:          "test-group",
	}

	var (
		errorCalled            int
		offsetsCommittedCalled int
	)

	type testCase struct {
		name                           string
		init                           func(h Handler, dlh DeadLetterHandler) *Consumer
		handlerFactory                 func() Handler
		deadHandlerFactory             func() DeadLetterHandler
		expectedHandlerCalled          int
		expectedDeadLetterCalled       int
		expectedErrorHandlerCalled     int
		expectedOffsetsCommittedCalled int
	}

	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     StringPtr("test"),
			Partition: 0,
			Offset:    1000,
		},
		Value:     []byte("Hello World"),
		Key:       []byte("hello111"),
		Timestamp: time.Now(),
		Headers:   []kafka.Header{{Key: "key", Value: []byte("value")}},
	}

	tests := []testCase{
		{
			name: "Run With Messages and Not-Fatal Errors",
			init: func(h Handler, dlh DeadLetterHandler) *Consumer {
				base := new(mockKafkaConsumer)

				// Setup Confluent Kafka Consumer to always succeed for the basic operations
				// like subscribing, storing messages, committing offsets, and closing.
				base.On("Subscribe", mock.Anything, mock.Anything).Return(nil)
				base.On("IsClosed").Return(false).Once()
				base.On("StoreMessage", mock.Anything).Return([]kafka.TopicPartition{}, nil)
				base.On("Commit").Return([]kafka.TopicPartition{}, nil)
				base.On("Close").Return(nil)

				// Simulate receiving messages with an error in between
				base.On("Poll", mock.Anything).Return(msg).Once()
				base.On("Poll", mock.Anything).Return(msg).Once()
				base.On("Poll", mock.Anything).Return(msg).Once()
				base.On("Poll", mock.Anything).Return(kafka.NewError(kafka.ErrAllBrokersDown, "something", false)).Once()
				base.On("Poll", mock.Anything).Return(msg).Once()
				base.On("Poll", mock.Anything).Return(msg).Once()

				// Simulate offsets committed event
				base.On("Poll", mock.Anything).Return(kafka.OffsetsCommitted{
					Error: nil,
					Offsets: []kafka.TopicPartition{
						{
							Topic:     StringPtr("test"),
							Partition: 0,
							Offset:    1000,
						},
					},
				}).Once()

				// Do nothing
				base.On("Poll", mock.Anything).Return(kafka.PartitionEOF{})

				consumer, err := NewConsumer(config, "test", h,
					WithDeadLetterHandler(dlh),
					WithOnOffsetsCommitted(func(offsets TopicPartitions, err error) {
						offsetsCommittedCalled++
					}),
					WithOnErr(func(err error) {
						errorCalled++
					}))
				assert.NoError(t, err)
				consumer.baseConsumer = base
				return consumer
			},
			handlerFactory: func() Handler {
				handler := new(mockHandler)
				handler.On("Handle", mock.Anything, mock.Anything).Return(nil).Once()
				handler.On("Handle", mock.Anything, mock.Anything).Return(nil).Once()
				handler.On("Handle", mock.Anything, mock.Anything).Return(nil).Once()
				handler.On("Handle", mock.Anything, mock.Anything).Return(nil).Once()
				handler.On("Handle", mock.Anything, mock.Anything).Return(assert.AnError).Once()
				return handler
			},
			deadHandlerFactory: func() DeadLetterHandler {
				dlh := new(mockDeadLetterHandler)
				dlh.On("Handle", mock.Anything, mock.Anything, mock.Anything).Return()
				return dlh
			},
			expectedHandlerCalled:          5,
			expectedDeadLetterCalled:       1,
			expectedErrorHandlerCalled:     1,
			expectedOffsetsCommittedCalled: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			errorCalled = 0
			offsetsCommittedCalled = 0

			handler := test.handlerFactory()
			deadLetterHandler := test.deadHandlerFactory()
			consumer := test.init(handler, deadLetterHandler)
			go func() {
				err := consumer.Run()
				assert.NoError(t, err)
			}()

			// todo: need a better way to handle this, this is flakey as hell
			time.Sleep(3 * time.Second)
			consumer.Close()

			assert.Equal(t, test.expectedHandlerCalled, len(handler.(*mockHandler).Calls))
			assert.Equal(t, test.expectedDeadLetterCalled, deadLetterHandler.(*mockDeadLetterHandler).called)
			assert.Equal(t, test.expectedErrorHandlerCalled, errorCalled)
			assert.Equal(t, test.expectedOffsetsCommittedCalled, offsetsCommittedCalled)
		})
	}
}

func TestConsumer(t *testing.T) {

	if !internal.IsTestContainersEnabled() {
		t.Skip("Integration tests are disabled. To enable, set TESTCONTAINERS_ENABLED=true in your environment.")
	}

	// --------------------------------------------------------------------------------------------
	// Spin up real Kafka using testcontainers
	// --------------------------------------------------------------------------------------------

	ctx := context.Background()
	kafkaContainer, err := kafkatest.Run(ctx,
		"confluentinc/confluent-local:7.5.0",
		kafkatest.WithClusterID("test-cluster"),
	)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := testcontainers.TerminateContainer(kafkaContainer); err != nil {
			t.Fatal(err)
		}
	}()

	// --------------------------------------------------------------------------------------------
	// Kafka client core configuration
	// --------------------------------------------------------------------------------------------

	brokers, err := kafkaContainer.Brokers(ctx)
	assert.NoError(t, err)

	config := KafkaConfig{
		BootstrapServers:       brokers,
		GroupID:                "test-group",
		AutoOffsetReset:        Earliest,
		AcknowledgmentStrategy: AcknowledgmentStrategyPostProcessing,
		RequiredAcks:           AckLeader,
	}

	// --------------------------------------------------------------------------------------------
	// Setup messages via the Producer so the Consumer has messages to Consume
	// --------------------------------------------------------------------------------------------

	producer, err := NewProducer(config)
	assert.NoError(t, err)

	keys := make([]string, 1000)

	for i := 0; i < 1000; i++ {
		id, err := uuid.NewV7()
		assert.NoError(t, err)
		key := id.String()
		keys[i] = key

		err = producer.M().
			Topic("test").
			Key(key).
			Value("Hello World!").
			Produce(context.Background())
		assert.NoError(t, err)
	}

	lastKey := keys[len(keys)-1]

	// --------------------------------------------------------------------------------------------
	// Consume messages
	// --------------------------------------------------------------------------------------------

	var consumer *Consumer

	processedKeys := make([]string, 0)
	handler := HandlerFunc(func(ctx context.Context, msg Message) error {
		processedKeys = append(processedKeys, string(msg.Key))
		if string(msg.Key) == lastKey {
			consumer.Close()
		}
		return nil
	})

	consumer, err = NewConsumer(config, "test", handler)
	assert.NoError(t, err)

	err = consumer.Run()
	assert.NoError(t, err)

	assert.ElementsMatch(t, keys, processedKeys)
}
