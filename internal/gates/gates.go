// Package gates reports integrations that were not executed.
// A set variable becomes blocked. This process does not dial it.
package gates

type Gate struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	Reason string `json:"reason"`
}

type item struct {
	name string
	env  string
	idle string
	busy string
}

var catalog = []item{
	{"postgres-remote", "LAB_POSTGRES_URL", "LAB_POSTGRES_URL is unset. A remote PostgreSQL was not contacted.", "LAB_POSTGRES_URL is set. This process does not connect."},
	{"redis-remote", "LAB_REDIS_URL", "LAB_REDIS_URL is unset. A remote Redis was not contacted.", "LAB_REDIS_URL is set. This process does not connect."},
	{"nats-remote", "LAB_NATS_URL", "LAB_NATS_URL is unset. A remote NATS cluster was not contacted.", "LAB_NATS_URL is set. This process does not connect."},
	{"oidc-provider", "LAB_OIDC_ISSUER", "LAB_OIDC_ISSUER is unset. No OIDC provider was contacted.", "LAB_OIDC_ISSUER is set. This process does not connect."},
	{"docker-local", "LAB_DOCKER", "Docker is not installed on this workstation. A local image build was not run.", "LAB_DOCKER is set. This process does not run Docker."},
	{"kubernetes", "LAB_KUBECONFIG", "kubectl is not installed. No manifest was applied.", "LAB_KUBECONFIG is set. This process does not use it."},
	{"customer-deployment", "LAB_CUSTOMER", "SYN-TENANT-QUORUM is synthetic. No customer deployment was made.", "LAB_CUSTOMER is set. This process does not deploy."},
}

func Evaluate(env map[string]string) []Gate {
	out := make([]Gate, 0, len(catalog))
	for _, item := range catalog {
		if env[item.env] == "" {
			out = append(out, Gate{Name: item.name, Status: "not-run", Reason: item.idle})
			continue
		}
		out = append(out, Gate{Name: item.name, Status: "blocked", Reason: item.busy})
	}
	return out
}
