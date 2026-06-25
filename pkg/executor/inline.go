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

package executor

import (
	"context"
	"fmt"
)

// InlineExecutor runs every submitted task in the caller goroutine.
type InlineExecutor struct{}

// NewInlineExecutor creates an inline executor.
func NewInlineExecutor() *InlineExecutor {
	return &InlineExecutor{}
}

// Submit runs the task immediately.
func (e *InlineExecutor) Submit(request SubmitRequest) error {
	if request.Context == nil {
		request.Context = context.Background()
	}
	if err := request.Context.Err(); err != nil {
		return err
	}
	if request.Task == nil {
		return fmt.Errorf("%w: runnable task is nil", ErrInvalid)
	}

	request.Task.Run(request.Context)
	return nil
}

// Shutdown is a no-op for InlineExecutor.
func (e *InlineExecutor) Shutdown(context.Context) error {
	return nil
}
