package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ansig/cdevents-jetstream-adapter/internal/adapter"
	"github.com/ansig/cdevents-jetstream-adapter/internal/translator"
	"github.com/ansig/cdevents-jetstream-adapter/internal/webhook"

	"github.com/kelseyhightower/envconfig"
	"github.com/nats-io/nats.go"
	natsjs "github.com/nats-io/nats.go/jetstream"
)

var logger *slog.Logger

var translators = map[string]translator.CDEventTranslator{
	"gitea.push":         &translator.GiteaPushTranslator{},
	"gitea.pull_request": &translator.GiteaPullRequestTranslator{},
	"gitea.create":       &translator.GiteaCreateTranslator{},
	"gitea.delete":       &translator.GiteaDeleteTranslator{},
}

type envConfig struct {
	HttpPort            int64  `envconfig:"HTTP_PORT" default:"8080" required:"true"`
	NATSUrl             string `envconfig:"NATS_URL" default:"http://localhost:4222" required:"true"`
	LogLevel            string `envconfig:"LOG_LEVEL" default:"info" required:"false"`
	WebhookStreamName   string `envconfig:"WEBHOOK_STREAM_NAME" default:"cdevents-adapter-webhooks" required:"true"`
	WebhookSubjectBase  string `envconfig:"WEBHOOK_SUBJECT_BASE" default:"webhooks" required:"true"`
	WebhookConsumerName string `envconfig:"WEBHOOK_CONSUMER_NAME" default:"cdevents-adapter" required:"true"`
	EventStreamName     string `envconfig:"EVENT_STREAM_NAME" default:"cdevents-adapter-events" required:"true"`
	EventSubjectBase    string `envconfig:"EVENT_SUBJECT_BASE" default:"dev.cdevents" required:"true"`
}

func MustCreateStream(ctx context.Context, jetstream natsjs.JetStream, config natsjs.StreamConfig) natsjs.Stream {

	var stream natsjs.Stream

	stream, err := jetstream.CreateStream(ctx, config)
	if err == natsjs.ErrStreamNameAlreadyInUse {
		logger.Info(fmt.Sprintf("Updating existing stream: %s", config.Name))
		stream, err = jetstream.UpdateStream(ctx, config)
		if err != nil {
			logger.Error("Failed to update existing stream", "error", err.Error())
			os.Exit(1)
		}
	} else if err != nil {
		logger.Error("Error when creating stream", "error", err.Error())
	}

	return stream
}

func main() {

	var env envConfig
	if err := envconfig.Process("", &env); err != nil {
		fmt.Printf("Error when processing envvar configuration: %v\n", err)
		os.Exit(1)
	}

	var programLevel = new(slog.LevelVar)
	logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: programLevel}))

	switch strings.ToLower(env.LogLevel) {
	case "debug":
		programLevel.Set(slog.LevelDebug)
	case "info":
		programLevel.Set(slog.LevelInfo)
	case "error":
		programLevel.Set(slog.LevelError)
	case "warn":
		programLevel.Set(slog.LevelWarn)
	default:
		logger.Warn(fmt.Sprintf("Unknown log level: %s (using default: %s)", env.LogLevel, programLevel.Level()))
	}

	logger.Info(fmt.Sprintf("Connecting to Nats on %s...", env.NATSUrl))

	nc, err := nats.Connect(env.NATSUrl)
	if err != nil {
		logger.Error("Failed to connect to nats", "error", err.Error())
		os.Exit(1)
	}

	defer nc.Close()

	jetstream, err := natsjs.New(nc)
	if err != nil {
		logger.Error("Failed to create JetStream instance", "error", err.Error())
		os.Exit(1)
	}

	startupCtx, startupCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer startupCancel()

	webhookSubject := fmt.Sprintf("%s.>", env.WebhookSubjectBase)

	WebhookStreamName := MustCreateStream(startupCtx, jetstream, natsjs.StreamConfig{
		Name:        env.WebhookStreamName,
		Subjects:    []string{webhookSubject},
		Description: "CDEvents adapter incoming webhook stream",
		Retention:   natsjs.WorkQueuePolicy,
	})

	eventSubject := fmt.Sprintf("%s.>", env.EventSubjectBase)

	MustCreateStream(startupCtx, jetstream, natsjs.StreamConfig{
		Name:        env.EventStreamName,
		Subjects:    []string{eventSubject},
		Description: "CDEvents adapter event output stream",
	})

	consumer, err := WebhookStreamName.CreateOrUpdateConsumer(startupCtx, natsjs.ConsumerConfig{
		Durable:   env.WebhookConsumerName,
		AckPolicy: natsjs.AckExplicitPolicy,
	})

	if err != nil {
		logger.Error("Failed to create consumer", "error", err.Error())
		os.Exit(1)
	}

	done := make(chan interface{})

	messages := make(chan natsjs.Msg)
	consContext, _ := consumer.Consume(func(msg natsjs.Msg) {
		messages <- msg
	})

	var wg sync.WaitGroup

	cdEventsAdapter := adapter.NewCDEventAdapter(logger, nc, translators)

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer consContext.Stop()
		for {
			select {
			case msg := <-messages:
				if err := cdEventsAdapter.Process(msg); err != nil {
					logger.Error("Error when processing message", "error", err.Error())
				}
			case <-done:
				logger.Info("Stopped processing messages")
				return
			}
		}
	}()

	logger.Info("JetStream consumer ready and listening...")

	logger.Info("Starting server...")

	webhook := webhook.NewHttpWebhook(logger)

	mux := http.NewServeMux()
	mux.Handle("/webhook", webhook.GetHandler(jetstream, env.WebhookSubjectBase))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if nc.IsConnected() {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("READY"))
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	})

	srv := http.Server{
		Addr:         fmt.Sprintf(":%d", env.HttpPort),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  90 * time.Second,
		Handler:      mux,
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		interrupt := make(chan os.Signal, 1)
		signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)
		s := <-interrupt

		logger.Info("Received interrupt signal", "signal", s)

		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), time.Second*10)
		defer cancelShutdown()

		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.Error("Error when shutting down server", "error", err.Error())
			os.Exit(1)
		}
	}()

	logger.Info(fmt.Sprintf("Server listening on port %d...", env.HttpPort))

	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		logger.Error("Error from listen and server", "error", err.Error())
		os.Exit(1)
	}

	close(done)

	logger.Info("Gracefully shutting down...")

	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()

	select {
	case <-c:
		logger.Info("All done, exit program")
	case <-time.After(time.Second * 30):
		logger.Error("Timeout waiting for all goroutines to finish")
		os.Exit(1)
	}
}
