package redact

import "testing"

func TestSecret(t *testing.T) {
	in := "Authorization: Bearer secret-value token eyJabc.def.ghi"
	out := Secret(in)
	if out == in || contains(out, "secret-value") || contains(out, "eyJabc") {
		t.Fatal(out)
	}
}

func contains(s, part string) bool {
	return len(part) > 0 && len(s) >= len(part) && (s == part || len(s) > 0 && indexOf(s, part) >= 0)
}

func indexOf(s, part string) int {
	for i := 0; i+len(part) <= len(s); i++ {
		if s[i:i+len(part)] == part {
			return i
		}
	}
	return -1
}
