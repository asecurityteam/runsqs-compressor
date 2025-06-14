package compression

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"fmt"
	"io/ioutil"

	"github.com/asecurityteam/runsqs/v3"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// DecompressionMessageConsumerConfig is the config for creating a DecompressionMessageConsumer
type DecompressionMessageConsumerConfig struct {
}

// Name of the config root.
func (*DecompressionMessageConsumerConfig) Name() string {
	return "sqsconsumerdecompression"
}

// DecompressionMessageConsumerComponent implements the settings.Component interface.
type DecompressionMessageConsumerComponent struct{}

// NewComponent populates default values.
func NewComponent() *DecompressionMessageConsumerComponent {
	return &DecompressionMessageConsumerComponent{}
}

// Settings generates a config populated with defaults.
func (*DecompressionMessageConsumerComponent) Settings() *DecompressionMessageConsumerConfig {
	return &DecompressionMessageConsumerConfig{}
}

// New genereates a DecompressionMessageConsumer decorate
func (c *DecompressionMessageConsumerComponent) New(_ context.Context, conf *DecompressionMessageConsumerConfig) (func(runsqs.SQSMessageConsumer) runsqs.SQSMessageConsumer, error) { // nolint
	return func(consumer runsqs.SQSMessageConsumer) runsqs.SQSMessageConsumer {
		return &DecompressionMessageConsumer{
			wrapped: consumer,
		}
	}, nil
}

// DecompressionMessageConsumer a is wrapper around runsqs.SQSMessageConsumer to capture and emit SQS related stats
type DecompressionMessageConsumer struct {
	wrapped runsqs.SQSMessageConsumer
}

// ConsumeMessage base64 decodes the message, and decompresses the message. It then calls the
// wrapped SQSMessageConsumer
func (t DecompressionMessageConsumer) ConsumeMessage(ctx context.Context, message *sqs.Message) runsqs.SQSMessageConsumerError {

	decompressedString, err := decompressString(*message.Body)
	if err != nil {
		return DecompressionMessageConsumerError{
			wrappedErr: err,
		}
	}
	message.Body = &decompressedString
	consumeErr := t.wrapped.ConsumeMessage(ctx, message)
	return consumeErr
}

func decompressString(message string) (string, error) {
	b := make([]byte, base64.StdEncoding.DecodedLen(len([]byte(message))))
	length, base64Err := base64.StdEncoding.Decode(b, []byte(message))
	if base64Err != nil {
		return "", base64Err
	}
	reader := bytes.NewReader(b[:length])
	gzreader, decompressionErr := gzip.NewReader(reader)
	if decompressionErr != nil {
		return "", decompressionErr
	}
	output, decompressionErr := ioutil.ReadAll(gzreader)
	if decompressionErr != nil {
		return "", decompressionErr

	}
	return string(output), nil
}

// DeadLetter decompresses/decodes the message and calls the wrapped DeadLetter
func (t DecompressionMessageConsumer) DeadLetter(ctx context.Context, message *sqs.Message) {
	t.wrapped.DeadLetter(ctx, message)
}

// NewDecompressionMessageConsumer returns a function that wraps a `runsqs.SQSMessageConsumer` in a
// `NewDecompressionMessageConsumer` `runsqs.SQSMessageConsumer`.
func NewDecompressionMessageConsumer() func(runsqs.SQSMessageConsumer) runsqs.SQSMessageConsumer {
	return func(consumer runsqs.SQSMessageConsumer) runsqs.SQSMessageConsumer {
		return &DecompressionMessageConsumer{wrapped: consumer}
	}
}

// DecompressionMessageConsumerError represents an error with DecompressionMessageConsumer
// This method implements runsqs.SQSMessageConsumerError
type DecompressionMessageConsumerError struct {
	wrappedErr error
}

// IsRetryable returns false, if decompression failures, it is a permenanet failure
func (e DecompressionMessageConsumerError) IsRetryable() bool {
	return false
}

// Error returns wrapped error
func (e DecompressionMessageConsumerError) Error() string {
	return fmt.Sprintf("DecompressionMessageConsumerError failure, wrapped error %s", e.wrappedErr)
}

// RetryAfter returns 0, if decompression failures, it is a permenanet failure
func (e DecompressionMessageConsumerError) RetryAfter() int64 {
	return 0
}
