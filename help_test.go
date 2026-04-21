package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRootHelp(t *testing.T) {
	var stdout bytes.Buffer
	if err := executeCLI([]string{"--help"}, strings.NewReader(""), &stdout, &stdout); err != nil {
		t.Fatal(err)
	}
	out := stdout.String()
	if !strings.Contains(out, "notify start [--name <name>]") || !strings.Contains(out, "notify stop [--name <name>] [--command <id>]") || !strings.Contains(out, "notify send [--name <name>]") || !strings.Contains(out, "notify command [--name <name>] [--recursive]") || !strings.Contains(out, "notify list") {
		t.Fatalf("unexpected help output: %q", out)
	}
}

func TestSubcommandHelp(t *testing.T) {
	tests := []struct {
		args []string
		want string
	}{
		{args: []string{"start", "--help"}, want: "notify start [--name <name>]"},
		{args: []string{"stop", "--help"}, want: "notify stop [--name <name>] [--command <id>]"},
		{args: []string{"send", "--help"}, want: "notify send [--name <name>]"},
		{args: []string{"command", "--help"}, want: "notify command [--name <name>] [--recursive] [--prefix \"<text>\"] \"<cmd>\""},
		{args: []string{"list", "--help"}, want: "notify list"},
	}

	for _, tt := range tests {
		var stdout bytes.Buffer
		if err := executeCLI(tt.args, strings.NewReader(""), &stdout, &stdout); err != nil {
			t.Fatalf("%v: %v", tt.args, err)
		}
		if !strings.Contains(stdout.String(), tt.want) {
			t.Fatalf("%v: expected help output to contain %q, got %q", tt.args, tt.want, stdout.String())
		}
	}
}
