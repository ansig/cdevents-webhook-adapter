package translator

import (
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/ansig/cdevents-jetstream-adapter/internal/structs"
	cdevents "github.com/cdevents/sdk-go/pkg/api"
	cdeventsv04 "github.com/cdevents/sdk-go/pkg/api/v04"
)

type GiteaPushTranslator struct{}

func (g *GiteaPushTranslator) Translate(data []byte) (cdevents.CDEvent, error) {

	var giteaEvent structs.GiteaPushEvent
	if err := json.Unmarshal(data, &giteaEvent); err != nil {
		return nil, err
	}

	if giteaEvent.TotalCommits == 0 {
		return nil, fmt.Errorf("Push event contains no new commits, will not convert to a CD Event")
	}

	cdEvent, err := cdeventsv04.NewChangeMergedEvent()
	if err != nil {
		return nil, err
	}

	addSourcesFromRepositoryUrl(giteaEvent, cdEvent)
	cdEvent.SetSubjectId(giteaEvent.Commits[0].Id)
	cdEvent.SetSubjectRepository(&cdevents.Reference{Id: giteaEvent.Repository.FullName})

	if err := addGiteaEventAsCustomData(giteaEvent, cdEvent); err != nil {
		return nil, err
	}

	return cdEvent, nil
}

type GiteaPullRequestTranslator struct{}

func (g *GiteaPullRequestTranslator) Translate(data []byte) (cdevents.CDEvent, error) {

	var giteaEvent structs.GiteaPullRequestEvent
	if err := json.Unmarshal(data, &giteaEvent); err != nil {
		return nil, err
	}

	var cdEvent cdevents.CDEvent

	switch giteaEvent.Action {
	case "opened":
		changeCreatedEvent, err := cdeventsv04.NewChangeCreatedEvent()
		if err != nil {
			return nil, err
		}
		changeCreatedEvent.SetSubjectRepository(&cdevents.Reference{Id: giteaEvent.Repository.FullName})
		cdEvent = changeCreatedEvent
	case "closed":
		changeMergedEvent, err := cdeventsv04.NewChangeMergedEvent()
		if err != nil {
			return nil, err
		}
		changeMergedEvent.SetSubjectRepository(&cdevents.Reference{Id: giteaEvent.Repository.FullName})
		cdEvent = changeMergedEvent
	default:
		return nil, fmt.Errorf("unsupported Gitea Pull Request action: %s", giteaEvent.Action)
	}

	addSourcesFromRepositoryUrl(giteaEvent, cdEvent)
	cdEvent.SetSubjectId(fmt.Sprintf("pr-%d", giteaEvent.PullRequest.Id))
	if err := cdEvent.SetCustomData("application/json", giteaEvent); err != nil {
		return nil, err
	}

	if err := addGiteaEventAsCustomData(giteaEvent, cdEvent); err != nil {
		return nil, err
	}

	return cdEvent, nil
}

type GiteaCreateTranslator struct{}

func (g *GiteaCreateTranslator) Translate(data []byte) (cdevents.CDEvent, error) {

	var giteaEvent structs.GiteaCreateEvent
	if err := json.Unmarshal(data, &giteaEvent); err != nil {
		return nil, err
	}

	var cdEvent cdevents.CDEvent

	switch giteaEvent.RefType {
	case "branch":
		branchCreatedEvent, err := cdeventsv04.NewBranchCreatedEvent()
		if err != nil {
			return nil, err
		}
		branchCreatedEvent.SetSubjectRepository(&cdevents.Reference{Id: giteaEvent.Repository.FullName})
		cdEvent = branchCreatedEvent
	default:
		return nil, fmt.Errorf("unsupported Gitea create ref type: %s", giteaEvent.RefType)
	}

	addSourcesFromRepositoryUrl(giteaEvent, cdEvent)
	cdEvent.SetSubjectId(giteaEvent.Ref)
	if err := cdEvent.SetCustomData("application/json", giteaEvent); err != nil {
		return nil, err
	}

	if err := addGiteaEventAsCustomData(giteaEvent, cdEvent); err != nil {
		return nil, err
	}

	return cdEvent, nil
}

type GiteaDeleteTranslator struct{}

func (g *GiteaDeleteTranslator) Translate(data []byte) (cdevents.CDEvent, error) {

	var giteaEvent structs.GiteaDeleteEvent
	if err := json.Unmarshal(data, &giteaEvent); err != nil {
		return nil, err
	}

	var cdEvent cdevents.CDEvent

	switch giteaEvent.RefType {
	case "branch":
		branchDeletedEvent, err := cdeventsv04.NewBranchDeletedEvent()
		if err != nil {
			return nil, err
		}
		branchDeletedEvent.SetSubjectRepository(&cdevents.Reference{Id: giteaEvent.Repository.FullName})
		cdEvent = branchDeletedEvent
	default:
		return nil, fmt.Errorf("unsupported Gitea create ref type: %s", giteaEvent.RefType)
	}

	addSourcesFromRepositoryUrl(giteaEvent, cdEvent)
	cdEvent.SetSubjectId(giteaEvent.Ref)

	if err := addGiteaEventAsCustomData(giteaEvent, cdEvent); err != nil {
		return nil, err
	}

	return cdEvent, nil
}

func addGiteaEventAsCustomData(giteaEvent interface{}, cdEvent cdevents.CDEvent) error {
	customData := struct {
		Kind    string
		Content interface{}
	}{
		Kind:    fmt.Sprintf("%T", giteaEvent),
		Content: giteaEvent,
	}
	if err := cdEvent.SetCustomData("application/json", customData); err != nil {
		return err
	}
	return nil
}

func addSourcesFromRepositoryUrl(giteaEvent interface{}, cdEvent cdevents.CDEvent) error {

	var rawRepoUrl string
	switch v := giteaEvent.(type) {
	case structs.GiteaCreateEvent:
		rawRepoUrl = v.Repository.HtmlUrl
	case structs.GiteaDeleteEvent:
		rawRepoUrl = v.Repository.HtmlUrl
	case structs.GiteaPushEvent:
		rawRepoUrl = v.Repository.HtmlUrl
	case structs.GiteaPullRequestEvent:
		rawRepoUrl = v.Repository.HtmlUrl
	default:
		panic(fmt.Sprintf("failed to extract repository URL from Gitea event with type: %T", giteaEvent))
	}

	repoUrl, err := url.Parse(rawRepoUrl)
	if err != nil {
		return err
	}

	cdEvent.SetSource(repoUrl.Host)

	subjectSource, err := url.JoinPath(repoUrl.Host, repoUrl.Path)
	if err != nil {
		return err
	}

	cdEvent.SetSubjectSource(subjectSource)

	return nil
}
