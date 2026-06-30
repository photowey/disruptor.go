// Copyright © 2026-present The Disruptor.go Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
		{name: "graph quickstart", dir: "graph_quickstart", expected: "validate:42,persist:42\n"},
		{name: "pipeline", dir: "pipeline", expected: "validate:7,enrich:7,persist:7\n"},
		{name: "diamond", dir: "diamond", expected: "diamond:D after B+C for 9\n"},
		{name: "graph export", dir: "graph_export", expected: "graph=export source=validate entry=validate leaf=persist exit=persist nodes=4 edges=3\n"},
		{name: "runtime graph", dir: "runtime_graph", expected: "route:11,fast:11\n"},
		{name: "runtime graph executor", dir: "runtime_graph_executor", expected: "route:31\nbranch:fraud:31\nbranch:pricing:31\npool-shutdown:caller\n"},
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
