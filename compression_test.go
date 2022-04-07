package compression

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/stretchr/testify/assert"

	"github.com/golang/mock/gomock"
)

func TestCompression(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockContext := context.Background()
	mockMessageProducer := NewMockSQSProducer(ctrl)

	compressionProducer := CompressionSQSProducer{
		Wrapped: mockMessageProducer,
	}
	originalMessage := "hello world!"
	encodedCompressedMessage := "H4sIAAAAAAAA/8pIzcnJVyjPL8pJUQQEAAD//23CtAMMAAAA"

	mockMessageProducer.EXPECT().ProduceMessage(mockContext, &sqs.SendMessageInput{
		MessageBody: &encodedCompressedMessage,
	}).Return(nil)

	e := compressionProducer.ProduceMessage(mockContext, &sqs.SendMessageInput{
		MessageBody: &originalMessage,
	})
	assert.Nil(t, e)

}
