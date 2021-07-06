package compression

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"fmt"

	"github.com/asecurityteam/runsqs"
)

// CompressionSQSProducer compresses the message, and calls the wrapped
// runsqs.SQSProducer to produce the compressed message.
// We must base64 encode the compressed message because SQS
// supports only character data -- valid utf8 -- not raw binary data
type CompressionSQSProducer struct {
	Wrapped runsqs.SQSProducer
}

// ProduceMessage produces a compressed, base64 encoded message to the configured sqs queue.
func (producer *CompressionSQSProducer) ProduceMessage(message []byte) error {
	var bytes bytes.Buffer
	gz := gzip.NewWriter(&bytes)
	if _, err := gz.Write(message); err != nil {
		return err
	}
	if err := gz.Close(); err != nil {
		return err
	}
	encodedBytes := make([]byte, base64.StdEncoding.EncodedLen(len(bytes.Bytes())))
	base64.StdEncoding.Encode(encodedBytes, bytes.Bytes())
	fmt.Println(string(encodedBytes))
	return producer.Wrapped.ProduceMessage(encodedBytes)
}
