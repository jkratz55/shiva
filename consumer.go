//   Copyright 2023 Joseph Kratz
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package shiva

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/segmentio/kafka-go"
)

const weight = 1

type Consumer struct {
	base    *kafka.Reader
	handler MessageHandler
	logger  Logger

	inConsumerGroup bool
}

func NewConsumer(brokers []string, groupID string, topic string) (*Consumer, error) {
	return nil, nil
}

func (c *Consumer) Consume() error {
	for {
		msg, err := c.base.FetchMessage(context.Background())
		if err != nil {
			c.logger.Error("Error fetching messages from Kafka",
				"err", err)
			if errors.Is(err, io.EOF) {
				// If FetchMessage returns an io.EOF error the reader has been
				// closed so there is no point trying to continue reading any
				// messages.
				return fmt.Errorf("reader has been closed: %w", err)
			}
		}

		err = c.handler.ProcessMessage(msg)
		if err != nil {
			c.logger.Error("MessageHandler returned a non-nil error value",
				"handler", fmt.Sprintf("%T", c.handler),
				"topic", msg.Topic,
				"partition", msg.Partition,
				"offset", msg.Offset,
				"key", string(msg.Key),
				"err", err)
		}

		if c.inConsumerGroup {
			err = c.base.CommitMessages(context.Background(), msg)
			if err != nil {
				c.logger.Error("Error committing message/offsets",
					"topic", msg.Topic,
					"partition", msg.Partition,
					"offset", msg.Offset,
					"key", string(msg.Key),
					"err", err)
			}
		}
	}
}

func (c *Consumer) Close() error {
	return c.base.Close()
}
