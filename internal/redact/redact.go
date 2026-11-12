// Package redact removes bearer tokens and compact JWTs from stored text.
package redact

import "regexp"

var (
	bearer = regexp.MustCompile(`Bearer\s+\S+`)
	jwt    = regexp.MustCompile(`eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+`)
)

func Secret(value string) string {
	value = bearer.ReplaceAllString(value, "Bearer [redacted]")
	return jwt.ReplaceAllString(value, "[redacted]")
}
