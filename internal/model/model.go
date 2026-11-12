// Package model holds the lab job and outcome types.
package model

type Job struct {
	ID       string
	TenantID string
	Kind     string
	Payload  []byte
	Token    string
}

type Outcome struct {
	ID       string `json:"id,omitempty"`
	Accepted bool   `json:"accepted"`
	State    string `json:"state"`
	Reason   string `json:"reason"`
	Result   string `json:"result,omitempty"`
}
