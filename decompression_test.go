package compression

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/stretchr/testify/assert"

	"github.com/golang/mock/gomock"
)

func TestDecompression(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSQSMessageConsumer := NewMockSQSMessageConsumer(ctrl)

	decompressionConsumer := DecompressionMessageConsumer{
		wrapped: mockSQSMessageConsumer,
	}
	originalMessage := "hello world!"
	encodedCompressedMessage := "H4sIAAAAAAAA/8pIzcnJVyjPL8pJUQQEAAD//23CtAMMAAAA"
	inboundSQSMessage := &sqs.Message{
		Body: &encodedCompressedMessage,
	}

	mockSQSMessageConsumer.EXPECT().ConsumeMessage(gomock.Any(), gomock.Any()).Return(nil)
	e := decompressionConsumer.ConsumeMessage(context.Background(), inboundSQSMessage)
	assert.Nil(t, e)
	assert.Equal(t, *inboundSQSMessage.Body, originalMessage)

}
