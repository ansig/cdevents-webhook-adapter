package translator

import (
	cdevents "github.com/cdevents/sdk-go/pkg/api"
)

type CDEventTranslator interface {
	Translate(data []byte) (cdevents.CDEvent, error)
}
