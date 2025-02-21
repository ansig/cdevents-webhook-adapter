package adapter

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/ansig/cdevents-jetstream-adapter/internal/translator"

	cdevents "github.com/cdevents/sdk-go/pkg/api"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	cejsm "github.com/cloudevents/sdk-go/protocol/nats_jetstream/v3"
	cloudevents "github.com/cloudevents/sdk-go/v2"
)

type CDEventPublisher interface {
	Publish(cdEvent cdevents.CDEvent) error
}

type CloudEventJetstreamPublisher struct {
	nc *nats.Conn
}

func (p *CloudEventJetstreamPublisher) Publish(cdEvent cdevents.CDEvent) error {
	cloudEvent, err := cdevents.AsCloudEvent(cdEvent)
	if err != nil {
		return err
	}

	connOpt := cejsm.WithConnection(p.nc)
	sendopt := cejsm.WithSendSubject(cloudEvent.Context.GetType())

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	proto, err := cejsm.New(ctx, connOpt, sendopt)
	if err != nil {
		return err
	}

	client, err := cloudevents.NewClient(proto)
	if err != nil {
		return err
	}

	if err := client.Send(ctx, *cloudEvent); err != nil {
		return err
	}

	return nil
}

type JetstreamMsg interface {
	Data() []byte
	Subject() string
	Ack() error
	Metadata() (*jetstream.MsgMetadata, error)
}

type CDEventAdapter struct {
	logger      *slog.Logger
	publisher   CDEventPublisher
	translators map[string]translator.CDEventTranslator
}

func NewCDEventAdapter(logger *slog.Logger, nc *nats.Conn, translators map[string]translator.CDEventTranslator) *CDEventAdapter {
	return &CDEventAdapter{
		logger:      logger,
		publisher:   &CloudEventJetstreamPublisher{nc: nc},
		translators: translators}
}

func (c *CDEventAdapter) Process(msg JetstreamMsg) error {

	defer msg.Ack()

	metadata, err := msg.Metadata()
	if err != nil {
		return err
	}

	c.logger.Debug("Processing incoming webhook message",
		"subject", msg.Subject(),
		"stream_seq", metadata.Sequence.Stream,
		"num_delivered", metadata.NumDelivered,
		"stream", metadata.Stream,
		"consumer", metadata.Consumer)

	var v map[string]interface{}
	if err := json.Unmarshal(msg.Data(), &v); err != nil {
		return err
	}

	subjectParts := strings.Split(msg.Subject(), ".")
	if len(subjectParts) < 2 {
		return fmt.Errorf("unable to determine type of message as subject has to few parts: %s", msg.Subject())
	}

	eventSubject := strings.Join(subjectParts[1:], ".")
	translator, exists := c.translators[eventSubject]
	if !exists {
		return fmt.Errorf("no translator found for subject: %s", eventSubject)
	}

	cdEvent, err := translator.Translate(msg.Data())
	if err != nil {
		return err
	}

	c.logger.Debug("Translated incoming webhook message into CDEvent",
		"type", cdEvent.GetType(),
		"subject", msg.Subject(),
		"stream_seq", metadata.Sequence.Stream,
		"num_delivered", metadata.NumDelivered,
		"stream", metadata.Stream,
		"consumer", metadata.Consumer)

	if err := c.publisher.Publish(cdEvent); err != nil {
		return err
	}

	return nil
}
