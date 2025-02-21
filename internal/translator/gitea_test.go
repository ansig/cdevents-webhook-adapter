package translator

import (
	"fmt"
	"testing"

	cdevents "github.com/cdevents/sdk-go/pkg/api"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGiteaPushTranslator(t *testing.T) {

	pushMainPayload := `{
		"ref": "refs/heads/main",
		"before": "a359287123178c5d05654864e80ab6f3bfc3d78a",
		"after": "9d7b2d18bf7f315c666a4b3607f47bd452e7c8d2",
		"commits": [
			{
				"id": "9d7b2d18bf7f315c666a4b3607f47bd452e7c8d2",
				"message": "Update README.md\n",
				"url": "http://git.example.com/yoloco/project1/commit/9d7b2d18bf7f315c666a4b3607f47bd452e7c8d2",
				"author": {
					"name": "anders",
					"email": "gi@tea.com",
					"username": "anders"
				},
				"committer": {
					"name": "anders",
					"email": "gi@tea.com",
					"username": "anders"
				},
				"timestamp": "2024-11-17T18:19:39Z",
				"added": [],
				"removed": [],
				"modified": [
					"README.md"
				]
			}
		],
		"total_commits": 1,
		"head_commit": {
			"id": "9d7b2d18bf7f315c666a4b3607f47bd452e7c8d2",
			"message": "Update README.md\n",
			"url": "http://git.example.com/yoloco/project1/commit/9d7b2d18bf7f315c666a4b3607f47bd452e7c8d2",
			"author": {
				"name": "anders",
				"email": "gi@tea.com",
				"username": "anders"
			},
			"committer": {
				"name": "anders",
				"email": "gi@tea.com",
				"username": "anders"
			},
			"timestamp": "2024-11-17T18:19:39Z",
			"added": [],
			"removed": [],
			"modified": [
				"README.md"
			]
		},
		"repository": {
			"full_name": "yoloco/project1",
			"html_url": "http://git.example.com/yoloco/project1",
			"ssh_url": "git@git.example.com:yoloco/project1.git"
		}
	}	
	`

	pushNewBranchPayload := `{
		"ref": "refs/heads/foo",
		"before": "0000000000000000000000000000000000000000",
		"after": "a5c0a10b8a2f5ce6b9ce27d8f63c411d06ededd5",
		"commits": [],
		"total_commits": 0,
		"head_commit": {
			"id": "9d7b2d18bf7f315c666a4b3607f47bd452e7c8d2",
			"message": "Update README.md\n",
			"url": "http://git.example.com/yoloco/project1/commit/9d7b2d18bf7f315c666a4b3607f47bd452e7c8d2",
			"author": {
				"name": "anders",
				"email": "gi@tea.com",
				"username": "anders"
			},
			"committer": {
				"name": "anders",
				"email": "gi@tea.com",
				"username": "anders"
			},
			"timestamp": "2024-11-17T18:19:39Z",
			"added": [],
			"removed": [],
			"modified": [
				"README.md"
			]
		},
		"repository": {
			"full_name": "yoloco/project1",
			"html_url": "http://git.example.com/yoloco/project1",
			"ssh_url": "git@git.example.com:yoloco/project1.git"
		}
	}`

	for _, tc := range []struct {
		title             string
		payload           string
		expectedEventType interface{}
		expectedError     error
	}{
		{
			title:             "returns ChangeMergedEvent on push to main branch payload",
			payload:           pushMainPayload,
			expectedEventType: cdevents.ChangeMergedEventTypeV0_2_0,
		},
		{
			title:         "error on push to new branch with no new commits",
			payload:       pushNewBranchPayload,
			expectedError: fmt.Errorf("Push event contains no new commits, will not convert to a CD Event"),
		},
	} {
		t.Run(tc.title, func(t *testing.T) {
			translator := &GiteaPushTranslator{}

			cdEvent, err := translator.Translate([]byte(tc.payload))

			if tc.expectedError != nil {
				assert.Equal(t, tc.expectedError, err)
			} else {
				require.NoError(t, err, "no error should be returned when translating event")
			}

			if tc.expectedEventType != nil {
				require.NotNil(t, cdEvent, "CD event must not be nil")

				assert.Equal(t, tc.expectedEventType, cdEvent.GetType(), "Event did not have expected type")
				assert.Equal(t, "9d7b2d18bf7f315c666a4b3607f47bd452e7c8d2", cdEvent.GetSubjectId(), "Subject ID must match head commit sha")
				assert.Equal(t, "git.example.com", cdEvent.GetSource(), "Event Source must be server host name")
				assert.Equal(t, "git.example.com/yoloco/project1", cdEvent.GetSubjectSource(), "Event Subject Source must be URL to project")

				subjectContent := cdEvent.GetSubjectContent()
				switch s := subjectContent.(type) {
				case cdevents.ChangeMergedSubjectContentV0_2_0:
					require.NotNil(t, s.Repository, "Content repository must not be nil")
					assert.Equal(t, "yoloco/project1", s.Repository.Id, "Content repository Id should be project full name")
				default:
					require.Fail(t, fmt.Sprintf("unexpected subject content type: %T", s))
				}
			}
		})
	}
}

func TestGiteaPullRequestTranslator(t *testing.T) {

	prOpenedPayload := `{
		"action": "opened",
		"number": 1,
		"pull_request": {
			"id": 3,
			"url": "http://git.example.com/yoloco/project1/pulls/1",
			"number": 1,
			"title": "Fix something PR",
			"base": {
				"label": "main",
				"ref": "main",
				"sha": "14a81e9adf2f116077ae960019448583a01fdde1"
			},
			"head": {
				"label": "foo",
				"ref": "foo",
				"sha": "9d7b2d18bf7f315c666a4b3607f47bd452e7c8d2"
			},
			"merge_base": "9d7b2d18bf7f315c666a4b3607f47bd452e7c8d2",
			"due_date": null,
			"created_at": "2024-11-17T18:21:54Z",
			"closed_at": null
		},
		"repository": {
			"id": 3,
			"owner": {
				"username": "yoloco"
			},
			"name": "project1",
			"full_name": "yoloco/project1",
			"html_url": "http://git.example.com/yoloco/project1",
			"ssh_url": "git@git.example.com:yoloco/project1.git"
		}
	}`

	prClosedPayload := `{
		"action": "closed",
		"number": 1,
		"pull_request": {
			"id": 3,
			"url": "http://git.example.com/yoloco/project1/pulls/1",
			"number": 1,
			"title": "Fix something PR",
			"base": {
				"label": "main",
				"ref": "main",
				"sha": "14a81e9adf2f116077ae960019448583a01fdde1"
			},
			"head": {
				"label": "foo",
				"ref": "foo",
				"sha": "14a81e9adf2f116077ae960019448583a01fdde1"
			},
			"created_at": "2024-11-17T18:21:54Z",
			"updated_at": "2024-11-17T18:24:31Z",
			"closed_at": "2024-11-17T18:24:31Z"
		},
		"repository": {
			"id": 3,
			"owner": {
				"username": "yoloco"
			},
			"name": "project1",
			"full_name": "yoloco/project1",
			"html_url": "http://git.example.com/yoloco/project1",
			"ssh_url": "git@git.example.com:yoloco/project1.git"
		}
	}
	`

	translator := &GiteaPullRequestTranslator{}

	for _, tc := range []struct {
		title               string
		payload             string
		expectedCDEventType cdevents.CDEventType
	}{
		{
			title:               "Return change created event on PR opened payload",
			payload:             prOpenedPayload,
			expectedCDEventType: cdevents.ChangeCreatedEventTypeV0_3_0,
		},
		{
			title:               "Return change merged event on PR closed payload",
			payload:             prClosedPayload,
			expectedCDEventType: cdevents.ChangeMergedEventTypeV0_2_0,
		},
	} {
		t.Run(tc.title, func(t *testing.T) {
			cdEvent, err := translator.Translate([]byte(tc.payload))

			require.NoError(t, err, "No error should be returned when translating event")

			require.NotNil(t, cdEvent, "CD event must not be nil")
			assert.Equal(t, tc.expectedCDEventType, cdEvent.GetType(), "Event must be of type ChangeCreatedEvent")
			assert.Equal(t, "git.example.com", cdEvent.GetSource(), "Event Source must be server host name")
			assert.Equal(t, "git.example.com/yoloco/project1", cdEvent.GetSubjectSource(), "Event Subject Source must be URL to project")
			assert.Equal(t, "pr-3", cdEvent.GetSubjectId(), "Subject Id should be pr-<number>")

			subjectContent := cdEvent.GetSubjectContent()
			switch s := subjectContent.(type) {
			case cdevents.ChangeCreatedSubjectContentV0_3_0:
				require.NotNil(t, s.Repository, "Content repository must not be nil")
				assert.Equal(t, "yoloco/project1", s.Repository.Id, "Content repository Id should be project full name")
			case cdevents.ChangeMergedSubjectContentV0_2_0:
				require.NotNil(t, s.Repository, "Content repository must not be nil")
				assert.Equal(t, "yoloco/project1", s.Repository.Id, "Content repository Id should be project full name")
			default:
				require.Fail(t, fmt.Sprintf("unexpected subject content type: %T", s))
			}
		})
	}
}

func TestGiteaCreateTranslator(t *testing.T) {
	payload := `{
		"sha": "9d7b2d18bf7f315c666a4b3607f47bd452e7c8d2",
		"ref": "foo",
		"ref_type": "branch",
		"repository": {
			"full_name": "yoloco/project1",
			"html_url": "http://git.example.com/yoloco/project1",
			"url": "http://git.example.com/api/v1/repos/yoloco/project1",
			"ssh_url": "git@git.example.com:yoloco/project1.git"
		}
  	}`

	translator := &GiteaCreateTranslator{}

	cdEvent, err := translator.Translate([]byte(payload))

	require.NoError(t, err, "no error should be returned when translating event")

	require.NotNil(t, cdEvent, "CD event must not be nil")
	assert.Equal(t, cdevents.BranchCreatedEventTypeV0_2_0, cdEvent.GetType(), "Event must be of type BranchCreatedEvent")
	assert.Equal(t, "foo", cdEvent.GetSubjectId(), "Subject ID must be name of ref")
	assert.Equal(t, "git.example.com", cdEvent.GetSource(), "Event Source must be server host name")
	assert.Equal(t, "git.example.com/yoloco/project1", cdEvent.GetSubjectSource(), "Event Subject Source must be URL to project")

	if content, ok := cdEvent.GetSubjectContent().(cdevents.BranchCreatedSubjectContentV0_2_0); ok {
		require.NotNil(t, content.Repository, "Content repository must not be nil")
		assert.Equal(t, "yoloco/project1", content.Repository.Id, "Content repository Id should be project full name")
	} else {
		require.Fail(t, "failed to cast Subject Content")
	}
}

func TestGiteaDeleteTranslator(t *testing.T) {
	payload := `{
		"ref": "foo",
		"ref_type": "branch",
		"repository": {
			"full_name": "yoloco/project1",
			"html_url": "http://git.example.com/yoloco/project1",
			"url": "http://git.example.com/api/v1/repos/yoloco/project1",
			"ssh_url": "git@git.example.com:yoloco/project1.git"
		}
  	}`

	translator := &GiteaDeleteTranslator{}

	cdEvent, err := translator.Translate([]byte(payload))

	require.NoError(t, err, "no error should be returned when translating event")

	require.NotNil(t, cdEvent, "CD event must not be nil")
	assert.Equal(t, cdevents.BranchDeletedEventTypeV0_2_0, cdEvent.GetType(), "Event must be of type BranchDeletedEvent")
	assert.Equal(t, "foo", cdEvent.GetSubjectId(), "Subject ID must be name of ref")
	assert.Equal(t, "git.example.com", cdEvent.GetSource(), "Event Source must be server host name")
	assert.Equal(t, "git.example.com/yoloco/project1", cdEvent.GetSubjectSource(), "Event Subject Source must be URL to project")

	if content, ok := cdEvent.GetSubjectContent().(cdevents.BranchDeletedSubjectContentV0_2_0); ok {
		require.NotNil(t, content.Repository, "Content repository must not be nil")
		assert.Equal(t, "yoloco/project1", content.Repository.Id, "Content repository Id should be project full name")
	} else {
		require.Fail(t, "failed to cast Subject Content")
	}
}
