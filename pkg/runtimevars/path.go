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

package runtimevars

import (
	"fmt"
	"strings"
)

type compiledPath struct {
	raw      string
	segments []string
}

func compilePath(path string) (compiledPath, error) {
	if err := ValidatePath(path); err != nil {
		return compiledPath{}, err
	}

	segmentCount := 1
	for index := 0; index < len(path); index++ {
		if path[index] == '.' {
			segmentCount++
		}
	}

	segments := make([]string, 0, segmentCount)
	start := 0
	for index := 0; index < len(path); index++ {
		if path[index] != '.' {
			continue
		}
		segments = append(segments, path[start:index])
		start = index + 1
	}
	segments = append(segments, path[start:])

	return compiledPath{
		raw:      path,
		segments: segments,
	}, nil
}

func (p compiledPath) String() string {
	return p.raw
}

func (p compiledPath) Len() int {
	return len(p.segments)
}

func (p compiledPath) At(index int) string {
	return p.segments[index]
}

// ValidatePath checks that a runtime variable path is non-empty and dot-safe.
func ValidatePath(path string) error {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return fmt.Errorf("runtime path is empty")
	}
	if trimmed != path {
		return fmt.Errorf("runtime path %q has surrounding whitespace", path)
	}
	previousDot := false
	for index := 0; index < len(path); index++ {
		if path[index] != '.' {
			previousDot = false
			continue
		}
		if index == 0 || previousDot {
			return fmt.Errorf("runtime path %q has an empty segment", path)
		}
		previousDot = true
	}
	if previousDot {
		return fmt.Errorf("runtime path %q has an empty segment", path)
	}

	return nil
}
