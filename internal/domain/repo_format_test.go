package domain

import "testing"

func TestRepoFormat_IsOCIRegistry(t *testing.T) {
	cases := []struct {
		format RepoFormat
		want   bool
	}{
		{FormatDocker, true},
		{FormatOCI, true},
		{FormatHelm, false},
		{FormatRaw, false},
		{FormatMaven2, false},
		{"", false},
		// The stored value is the canonical lowercase label; anything else is not
		// a format this codebase writes, and the check stays strict.
		{"Docker", false},
		{"OCI", false},
	}
	for _, tc := range cases {
		if got := tc.format.IsOCIRegistry(); got != tc.want {
			t.Errorf("RepoFormat(%q).IsOCIRegistry() = %v, want %v", tc.format, got, tc.want)
		}
	}
}

func TestIsDockerPathComponent(t *testing.T) {
	cases := []struct {
		name string
		want bool
	}{
		{"alpine", true},
		{"docker-test", true},
		// Proxy repositories are named after the registry they front.
		{"quay.io", true},
		{"registry-1.docker.io", true},
		{"team.registry_2", true},
		{"a--b", true},
		{"", false},
		{"docker test", false},
		{"Alpine", false},
		// Separators cannot lead, trail or double up, which is what keeps the
		// name safe to splice into a URL path.
		{"-alpine", false},
		{"alpine-", false},
		{"a..b", false},
		{"a/b", false},
		{"../../admin", false},
	}
	for _, tc := range cases {
		if got := IsDockerPathComponent(tc.name); got != tc.want {
			t.Errorf("IsDockerPathComponent(%q) = %v, want %v", tc.name, got, tc.want)
		}
	}
}
