package identity

import "strings"

// Bearer returns the credential from an Authorization header.
func Bearer(header string) string {
	fields := strings.Fields(header)
	if len(fields) != 2 || !strings.EqualFold(fields[0], "bearer") {
		return ""
	}
	return fields[1]
}
