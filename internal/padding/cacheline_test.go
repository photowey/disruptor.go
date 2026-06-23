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

package padding

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"unsafe"
)

const cacheLineOverrideHelperEnv = "DISRUPTOR_CACHELINE_OVERRIDE_HELPER"

func TestCacheLineSizeMatchesArchitectureDefault(t *testing.T) {
	if cacheLineSizeOverridden {
		t.Skip("cache line size is overridden by build tag")
	}

	expected := expectedCacheLineSize(runtime.GOARCH)
	if CacheLineSize != expected {
		t.Fatalf("cache line size = %d, want %d for %s", CacheLineSize, expected, runtime.GOARCH)
	}
	if got := int(unsafe.Sizeof(CacheLine{})); got != CacheLineSize {
		t.Fatalf("cache line type size = %d, want %d", got, CacheLineSize)
	}
}

func TestCacheLineBuildTagOverrides(t *testing.T) {
	if os.Getenv(cacheLineOverrideHelperEnv) != "" {
		return
	}

	for _, size := range []int{32, 64, 128, 256} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			cmd := exec.Command(
				"go",
				"test",
				"-run",
				"^TestCacheLineOverrideHelper$",
				"-tags",
				fmt.Sprintf("disruptor_cacheline_%d", size),
				"./internal/padding",
			)
			cmd.Dir = repositoryRoot(t)
			cmd.Env = append(os.Environ(), fmt.Sprintf("%s=%d", cacheLineOverrideHelperEnv, size))
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("cacheline override %d failed: %v\n%s", size, err, output)
			}
		})
	}
}

func TestCacheLineOverrideHelper(t *testing.T) {
	value := os.Getenv(cacheLineOverrideHelperEnv)
	if value == "" {
		t.Skip("helper test runs in a subprocess")
	}

	expected, err := strconv.Atoi(value)
	if err != nil {
		t.Fatalf("parse helper cache line size: %v", err)
	}
	if CacheLineSize != expected {
		t.Fatalf("cache line size = %d, want override %d", CacheLineSize, expected)
	}
	if got := int(unsafe.Sizeof(CacheLine{})); got != expected {
		t.Fatalf("cache line type size = %d, want override %d", got, expected)
	}
}

func expectedCacheLineSize(goarch string) int {
	switch goarch {
	case "arm", "mips", "mipsle", "mips64", "mips64le":
		return 32
	case "arm64", "ppc64", "ppc64le":
		return 128
	case "s390x":
		return 256
	default:
		return 64
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()

	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate test file")
	}

	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}
