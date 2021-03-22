package forwarder

import (
	"github.com/DataDog/datadog-agent/pkg/logs/message"
	"github.com/stretchr/testify/mock"
)

type MockEventPlatformForwarder struct {
	mock.Mock
}

func (s *MockEventPlatformForwarder) SendEventPlatformEvent(e *message.Message, track string) error {
	return nil
}
