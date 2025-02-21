package structs

type GiteaPushEvent struct {
	Ref          string   `json:"ref"`
	Before       string   `json:"before"`
	After        string   `json:"after"`
	Commits      []commit `json:"commits"`
	TotalCommits int      `json:"total_commits"`
	HeadCommit   commit   `json:"head_commit"`
	commonFields
}

type GiteaPullRequestEvent struct {
	Action      string      `json:"action"`
	Number      int         `json:"number"`
	PullRequest pullRequest `json:"pull_request"`
	commonFields
}

type GiteaCreateEvent struct {
	Sha     string `json:"sha"`
	Ref     string `json:"ref"`
	RefType string `json:"ref_type"`
	commonFields
}

type GiteaDeleteEvent struct {
	Ref     string `json:"ref"`
	RefType string `json:"ref_type"`
	commonFields
}

type commonFields struct {
	Repository struct {
		Name  string `json:"name"`
		Owner struct {
			Username string `json:"username"`
		} `json:"owner"`
		FullName string `json:"full_name"`
		Url      string `json:"url"`
		HtmlUrl  string `json:"html_url"`
		SshUrl   string `json:"ssh_url"`
	} `json:"repository"`
}

type authorCommitter struct {
	Name     string `json:"name"`
	Email    string `json:"email"`
	Username string `json:"username"`
}

type commit struct {
	Id        string          `json:"id"`
	Message   string          `json:"message"`
	Timestamp string          `json:"timestamp"`
	Author    authorCommitter `json:"author"`
	Committer authorCommitter `json:"committer"`
}

type pullRequest struct {
	Id        int            `json:"id"`
	Title     string         `json:"title"`
	Base      pullRequestRef `json:"base"`
	Head      pullRequestRef `json:"head"`
	CreatedAt string         `json:"created_at"`
	UpdatedAt string         `json:"updated_at"`
	ClosedAt  string         `json:"closed_at"`
}

type pullRequestRef struct {
	Label string `json:"label"`
	Ref   string `json:"ref"`
	Sha   string `json:"sha"`
}
