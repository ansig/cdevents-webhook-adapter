package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type JetStreamClient interface {
	Publish(ctx context.Context, subject string, data []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error)
}

type HttpWebhook struct {
	logger *slog.Logger
}

func NewHttpWebhook(logger *slog.Logger) *HttpWebhook {
	return &HttpWebhook{logger: logger}
}

func (s *HttpWebhook) GetHandler(jsClient JetStreamClient, subjectBase string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not supported", http.StatusNotImplemented)
			return
		}

		contentType := r.Header.Get("Content-Type")
		if contentType == "" {
			http.Error(w, "Content-Type header not set", http.StatusBadRequest)
			return
		}

		mt, _, err := mime.ParseMediaType(contentType)
		if err != nil {
			http.Error(w, "Malformed Content-Type header", http.StatusBadRequest)
			return
		}

		if mt != "application/json" {
			http.Error(w, "Content-Type header must be application/json", http.StatusUnsupportedMediaType)
			return
		}

		var subject string
		giteaEventHeader := r.Header.Get("X-Gitea-Event")
		if giteaEventHeader != "" {
			s.logger.Debug(fmt.Sprintf("Setting message subject based on X-Gitea-Event header: %s", giteaEventHeader))
			subject = fmt.Sprintf("%s.gitea.%s", subjectBase, giteaEventHeader)
		} else {
			subject = fmt.Sprintf("%s.unknown", subjectBase)
			s.logger.Warn(fmt.Sprintf("Found no known headers on which to route incoming webhook message, sending to subject: %s", subject))
		}

		data, err := io.ReadAll(r.Body)
		if err != nil {
			s.logger.Error("Failure when reading request body", "error", err.Error())
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		if len(data) == 0 {
			http.Error(w, "Received empty body", http.StatusBadRequest)
			return
		}

		var v map[string]interface{}
		if err := json.Unmarshal(data, &v); err != nil {
			http.Error(w, "Payload is not valid json", http.StatusBadRequest)
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		s.logger.Debug(fmt.Sprintf("Publishing incoming webhook to Jetstream subject: %s", subject))

		_, err = jsClient.Publish(ctx, subject, data)
		if err != nil {
			s.logger.Error("Error when publishing event to Jetstream", "error", err.Error())
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
}
