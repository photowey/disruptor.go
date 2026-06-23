package examples_test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestExamplesRun(t *testing.T) {
	tests := []struct {
		name     string
		dir      string
		expected string
	}{
		{name: "basic", dir: "basic", expected: "42\n"},
		{name: "multi consumer", dir: "multi_consumer", expected: "audit:1001,projection:1001\n"},
		{name: "metrics", dir: "metrics", expected: "published=1 handled=1\n"},
		{name: "error recovery", dir: "error_recovery", expected: "value=9 attempts=3\n"},
		{name: "batch publish", dir: "batch_publish", expected: "batch=1,2,3,4 sum=10\n"},
		{name: "single producer", dir: "single_producer", expected: "single=7\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			command := exec.Command("go", "run", "./"+tt.dir)
			command.Env = append(os.Environ(), "CGO_ENABLED=0")

			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("go run ./examples/%s: %v\n%s", tt.dir, err, output)
			}
			if string(output) != tt.expected {
				t.Fatalf("output = %q, want %q", output, tt.expected)
			}
		})
	}
}

func TestExamplesAvoidAnonymousFunctions(t *testing.T) {
	err := filepath.WalkDir(".", func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || filepath.Base(path) != "main.go" {
			return nil
		}

		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if bytes.Contains(content, []byte("func(")) {
			t.Fatalf("%s contains anonymous function syntax", path)
		}
		if strings.Contains(string(content), "Func[") {
			t.Fatalf("%s uses a XxxFunc adapter instead of a named type", path)
		}

		return nil
	})
	if err != nil {
		t.Fatalf("walk examples: %v", err)
	}
}
