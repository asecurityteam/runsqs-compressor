package compression

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"

	"github.com/asecurityteam/runsqs/v2"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// CompressionSQSProducer compresses the message, and calls the wrapped
// runsqs.SQSProducer to produce the compressed message.
// We must base64 encode the compressed message because SQS
// supports only character data -- valid utf8 -- not raw binary data
type CompressionSQSProducer struct {
	Wrapped runsqs.SQSProducer
}

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
