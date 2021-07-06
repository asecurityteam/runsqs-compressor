package compression

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/golang/mock/gomock"
)

func TestCompression(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMessageProducer := NewMockSQSProducer(ctrl)

	compressionProducer := CompressionSQSProducer{
		Wrapped: mockMessageProducer,
	}
	originalMessage := "hello world!"
	encodedCompressedMessage := "H4sIAAAAAAAA/8pIzcnJVyjPL8pJUQQEAAD//23CtAMMAAAA"

	mockMessageProducer.EXPECT().ProduceMessage([]byte(encodedCompressedMessage)).Return(nil)
	e := compressionProducer.ProduceMessage([]byte(originalMessage))
	assert.Nil(t, e)

}
