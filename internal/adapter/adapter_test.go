package adapter

import (
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/ansig/cdevents-jetstream-adapter/internal/translator"
	cdevents "github.com/cdevents/sdk-go/pkg/api"
	cdeventsv04 "github.com/cdevents/sdk-go/pkg/api/v04"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type MockCDEventPublisher struct {
	mock.Mock
}

func (m *MockCDEventPublisher) Publish(cdEvent cdevents.CDEvent) error {
	args := m.Called(cdEvent)
	return args.Error(0)
}

type MockJetstreamMsg struct {
	mock.Mock
	subject      string
	data         []byte
	acked        bool
	consumerSeq  uint64
	streamSeq    uint64
	numDelivered uint64
}

func (m *MockJetstreamMsg) Subject() string { return m.subject }
func (m *MockJetstreamMsg) Data() []byte    { return m.data }
func (m *MockJetstreamMsg) Ack() error {
	m.acked = true
	return nil
}
func (m *MockJetstreamMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{
		Sequence: jetstream.SequencePair{
			Stream:   m.streamSeq,
			Consumer: m.consumerSeq,
		},
		NumDelivered: m.numDelivered,
	}, nil
}

func newMockJetstreamMsg(subject string, data []byte) *MockJetstreamMsg {
	return &MockJetstreamMsg{
		subject: subject,
		data:    data,
	}
}

type MockCDEventTranslator struct {
	mock.Mock
}

func (m *MockCDEventTranslator) Translate(data []byte) (cdevents.CDEvent, error) {
	args := m.Called(data)
	return args.Get(0).(cdevents.CDEvent), args.Error(1)
}

func TestProcess(t *testing.T) {

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	for _, tc := range []struct {
		title                   string
		msgSubject              string
		msgData                 []byte
		translatorSubject       string
		translateReturnsEvent   bool
		expectedError           error
		expectEventPublished    bool
		expectEventNotPublished bool
		expectMsgDataTranslated bool
	}{
		{
			title:                   "translates message data and publishes translated event",
			msgSubject:              "webhook.test.event",
			msgData:                 []byte("{\"foo\": \"bar\"}"),
			translatorSubject:       "test.event",
			expectEventPublished:    true,
			expectMsgDataTranslated: true,
		},
		{
			title:                   "error when no translator matching subject",
			msgSubject:              "webhook.test.foo",
			msgData:                 []byte("{\"foo\": \"bar\"}"),
			translatorSubject:       "test.bar",
			expectedError:           fmt.Errorf("no translator found for subject: test.foo"),
			expectEventNotPublished: true,
		},
		{
			title:                   "error on less than 2 subject parts",
			msgSubject:              "webhook",
			msgData:                 []byte("{\"foo\": \"bar\"}"),
			translatorSubject:       "test.bar",
			expectedError:           fmt.Errorf("unable to determine type of message as subject has to few parts: webhook"),
			expectEventNotPublished: true,
		},
	} {
		t.Run(tc.title, func(t *testing.T) {
			mockPublisher := &MockCDEventPublisher{}
			mockTranslator := &MockCDEventTranslator{}

			adapter := &CDEventAdapter{
				logger:      logger,
				publisher:   mockPublisher,
				translators: map[string]translator.CDEventTranslator{tc.translatorSubject: mockTranslator},
			}

			cde, err := cdeventsv04.NewChangeMergedEvent()
			require.NoError(t, err, "unable to create CDEvent for tests")

			var expectedData interface{}
			if tc.expectMsgDataTranslated {
				expectedData = tc.msgData
			} else {
				expectedData = mock.Anything
			}

			mockTranslator.On("Translate", expectedData).Return(cde, nil)

			var expectedEvent interface{}
			if tc.expectEventPublished {
				expectedEvent = cde
			} else {
				expectedEvent = mock.Anything
			}
			mockPublisher.On("Publish", expectedEvent).Return(nil)

			msg := newMockJetstreamMsg(tc.msgSubject, tc.msgData)

			err = adapter.Process(msg)

			if tc.expectedError != nil {
				require.Equal(t, tc.expectedError, err, "did not return expected error")
			} else {
				require.NoError(t, err, "no error should be returned")
			}

			if tc.expectMsgDataTranslated {
				mockTranslator.AssertCalled(t, "Translate", expectedData)
			}

			if tc.expectEventPublished {
				mockPublisher.AssertCalled(t, "Publish", expectedEvent)
			}

			if tc.expectEventNotPublished {
				mockPublisher.AssertNotCalled(t, "Publish", expectedEvent)
			}
		})
	}
}
