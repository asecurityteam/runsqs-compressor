package compression

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/stretchr/testify/assert"

	"github.com/golang/mock/gomock"
)

const (
	helloWorld               = `hello world!`
	encodedCompressedMessage = `H4sIAAAAAAAA/8pIzcnJVyjPL8pJUQQEAAD//23CtAMMAAAA`
	exampleURL               = `www.example.com`
)

func TestCompressionProduceMessage(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockContext := context.Background()
	mockMessageProducer := NewMockSQSProducer(ctrl)

	compressionProducer := CompressionSQSProducer{
		Wrapped: mockMessageProducer,
	}
	queueURL := exampleURL
	originalMessage := helloWorld
	encodedCompressedMessage := encodedCompressedMessage

	mockMessageProducer.EXPECT().QueueURL().Return(queueURL)
	mockMessageProducer.EXPECT().ProduceMessage(mockContext, &sqs.SendMessageInput{
		QueueUrl:    &queueURL,
		MessageBody: &encodedCompressedMessage,
	}).Return(nil)

	e := compressionProducer.ProduceMessage(mockContext, &sqs.SendMessageInput{
		MessageBody: &originalMessage,
	})
	assert.Nil(t, e)

}

func TestCompressionBatchProduceMessage(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockContext := context.Background()
	mockMessageProducer := NewMockSQSProducer(ctrl)

	compressionProducer := CompressionSQSProducer{
		Wrapped: mockMessageProducer,
	}
	queueURL := exampleURL
	originalMessage := helloWorld
	encodedCompressedMessage := encodedCompressedMessage

	mockMessageProducer.EXPECT().QueueURL().Return(queueURL)
	mockMessageProducer.EXPECT().BatchProduceMessage(mockContext, &sqs.SendMessageBatchInput{
		QueueUrl: &queueURL,
		Entries: []*sqs.SendMessageBatchRequestEntry{{
			MessageBody: &encodedCompressedMessage,
		}},
	}).Return(nil, nil)

	_, e := compressionProducer.BatchProduceMessage(mockContext, &sqs.SendMessageBatchInput{
		QueueUrl: &queueURL,
		Entries: []*sqs.SendMessageBatchRequestEntry{{
			MessageBody: &originalMessage,
		}},
	})
	assert.Nil(t, e)

}
