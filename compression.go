package compression

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"

	"github.com/asecurityteam/runsqs/v3"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// CompressionSQSProducer compresses the message, and calls the wrapped
// runsqs.SQSProducer to produce the compressed message.
// We must base64 encode the compressed message because SQS
// supports only character data -- valid utf8 -- not raw binary data
type CompressionSQSProducer struct { // nolint
	Wrapped runsqs.SQSProducer
}

// QueueURL retrieves the queue URL used by the wrapped SQS producer.
func (producer *CompressionSQSProducer) QueueURL() string {
	return producer.Wrapped.QueueURL()
}

// ProduceMessage produces a compressed, base64 encoded message to the configured sqs queue.
func (producer *CompressionSQSProducer) ProduceMessage(ctx context.Context, messageInput *sqs.SendMessageInput) error {
	var bytes bytes.Buffer
	gz := gzip.NewWriter(&bytes)
	if _, err := gz.Write([]byte(*messageInput.MessageBody)); err != nil {
		return err
	}
	if err := gz.Close(); err != nil {
		return err
	}
	encodedBytes := make([]byte, base64.StdEncoding.EncodedLen(len(bytes.Bytes())))
	base64.StdEncoding.Encode(encodedBytes, bytes.Bytes())

	messageInput.QueueUrl = aws.String(producer.Wrapped.QueueURL())
	messageInput.MessageBody = aws.String(string(encodedBytes))
	return producer.Wrapped.ProduceMessage(ctx, messageInput)
}

// BatchProduceMessage produces compressed, base64 encoded messages to the configured sqs queue.
func (producer *CompressionSQSProducer) BatchProduceMessage(ctx context.Context, messageBatchInput *sqs.SendMessageBatchInput) (*sqs.SendMessageBatchOutput, error) {
	for i := range messageBatchInput.Entries {
		var bytes bytes.Buffer
		gz := gzip.NewWriter(&bytes)
		if _, err := gz.Write([]byte(*messageBatchInput.Entries[i].MessageBody)); err != nil {
			return nil, err
		}
		if err := gz.Close(); err != nil {
			return nil, err
		}
		encodedBytes := make([]byte, base64.StdEncoding.EncodedLen(len(bytes.Bytes())))
		base64.StdEncoding.Encode(encodedBytes, bytes.Bytes())

		messageBatchInput.QueueUrl = aws.String(producer.Wrapped.QueueURL())
		messageBatchInput.Entries[i].MessageBody = aws.String(string(encodedBytes))
	}
	return producer.Wrapped.BatchProduceMessage(ctx, messageBatchInput)
}
