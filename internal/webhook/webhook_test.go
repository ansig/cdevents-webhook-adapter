package webhook

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/mock"
)

type MockJetStreamClient struct {
	mock.Mock
}

func (m *MockJetStreamClient) Publish(ctx context.Context, subject string, data []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	args := m.Called(subject, data)
	return args.Get(0).(*jetstream.PubAck), args.Error(1)
}

type httpWebhookHandlerTC struct {
	title                  string
	requestMethod          string
	requestBody            string
	requestHeaders         map[string][]string
	jetstreamSubjectBase   string
	expectedPublishSubject string
	expectedPublishData    string
	expectedResponseCode   int
	expectedResponseBody   string
}

func newDefaultWebhookHandlerTC() httpWebhookHandlerTC {
	return httpWebhookHandlerTC{
		requestMethod: http.MethodPost,
		requestBody:   "{\"foo\": \"bar\"}",
		requestHeaders: map[string][]string{
			"Content-Type": {"application/json"},
		},
		expectedResponseCode: http.StatusOK,
		expectedResponseBody: `OK`,
	}
}

func TestHttpWebhookHandler(t *testing.T) {

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	webhook := NewHttpWebhook(logger)

	for _, tc := range []httpWebhookHandlerTC{
		func() httpWebhookHandlerTC {
			tc := newDefaultWebhookHandlerTC()
			tc.title = "ok on POST request with valid headers and JSON payload"
			return tc
		}(),
		func() httpWebhookHandlerTC {
			tc := newDefaultWebhookHandlerTC()
			tc.title = "error on GET request"
			tc.requestMethod = http.MethodGet
			tc.expectedResponseCode = http.StatusNotImplemented
			tc.expectedResponseBody = `Method not supported`
			return tc
		}(),
		func() httpWebhookHandlerTC {
			tc := newDefaultWebhookHandlerTC()
			tc.title = "error without Content-Type header"
			tc.requestHeaders = map[string][]string{}
			tc.expectedResponseCode = http.StatusBadRequest
			tc.expectedResponseBody = `Content-Type header not set`
			return tc
		}(),
		func() httpWebhookHandlerTC {
			tc := newDefaultWebhookHandlerTC()
			tc.title = "error with malformed Content-Type header"
			tc.requestHeaders = map[string][]string{
				"Content-Type": {"malformed;thing"},
			}
			tc.expectedResponseCode = http.StatusBadRequest
			tc.expectedResponseBody = `Malformed Content-Type header`
			return tc
		}(),
		func() httpWebhookHandlerTC {
			tc := newDefaultWebhookHandlerTC()
			tc.title = "error with empty body"
			tc.requestBody = ""
			tc.expectedResponseCode = http.StatusBadRequest
			tc.expectedResponseBody = `Received empty body`
			return tc
		}(),
		func() httpWebhookHandlerTC {
			tc := newDefaultWebhookHandlerTC()
			tc.title = "error with invalid Json body"
			tc.requestBody = "notvalidjson"
			tc.expectedResponseCode = http.StatusBadRequest
			tc.expectedResponseBody = `Payload is not valid json`
			return tc
		}(),
		func() httpWebhookHandlerTC {
			tc := newDefaultWebhookHandlerTC()
			tc.title = "publish to subject test.unknown without any known headers"
			tc.jetstreamSubjectBase = "test"
			tc.expectedPublishSubject = "test.unknown"
			return tc
		}(),
		func() httpWebhookHandlerTC {
			tc := newDefaultWebhookHandlerTC()
			tc.title = "publish to subject test.gitea.push with X-Gitea-Event header"
			tc.requestHeaders["X-Gitea-Event"] = []string{"push"}
			tc.jetstreamSubjectBase = "test"
			tc.expectedPublishSubject = "test.gitea.push"
			return tc
		}(),
	} {
		t.Run(tc.title, func(t *testing.T) {

			reader := strings.NewReader(tc.requestBody)

			req := httptest.NewRequest(tc.requestMethod, "/", reader)
			for k, v := range tc.requestHeaders {
				req.Header.Add(k, strings.Join(v, ","))
			}
			rec := httptest.NewRecorder()

			mockJS := &MockJetStreamClient{}

			var expectedSubject string
			if tc.expectedPublishSubject != "" {
				expectedSubject = tc.expectedPublishSubject
			} else {
				expectedSubject = mock.Anything
			}

			var expectedData []byte
			if tc.expectedPublishData != "" {
				expectedData = []byte(tc.expectedPublishData)
			} else {
				expectedData = []byte(tc.requestBody)
			}

			mockJS.On("Publish", expectedSubject, expectedData).Return(&jetstream.PubAck{Stream: "mockStream"}, nil)

			webhook.GetHandler(mockJS, tc.jetstreamSubjectBase).ServeHTTP(rec, req)

			res := rec.Result()
			defer res.Body.Close()

			if res.StatusCode != tc.expectedResponseCode {
				t.Errorf("expected status OK; got %d", res.StatusCode)
			}

			body, _ := io.ReadAll(res.Body)
			if strings.TrimSpace(string(body)) != tc.expectedResponseBody {
				t.Errorf("expected body %q; got %q", tc.expectedResponseBody, body)
			}
		})
	}
}
