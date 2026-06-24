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

// ValidatePath checks that a runtime variable path is non-empty and dot-safe.
func ValidatePath(path string) error {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return fmt.Errorf("runtime path is empty")
	}
	if trimmed != path {
		return fmt.Errorf("runtime path %q has surrounding whitespace", path)
	}
	for _, part := range strings.Split(path, ".") {
		if part == "" {
			return fmt.Errorf("runtime path %q has an empty segment", path)
		}
	}

	return nil
}
