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
	"strings"
)

// Executor accepts runnable tasks and owns their execution strategy.
type Executor interface {
	Submit(request SubmitRequest) error
	Shutdown(ctx context.Context) error
}

// SubmitRequest describes a task submission.
type SubmitRequest struct {
	Context context.Context
	Task    RunnableTask
	Name    string
}

// RunnableTask is the non-generic task shape used by Executor.
type RunnableTask interface {
	Run(ctx context.Context)
}

// RunnableTaskFunc adapts a named function value to RunnableTask.
type RunnableTaskFunc func(ctx context.Context)

// Run calls the wrapped function.
func (fn RunnableTaskFunc) Run(ctx context.Context) {
	if fn != nil {
		fn(ctx)
	}
}

// NoopTask is a runnable task that does nothing.
type NoopTask struct{}

// Run implements RunnableTask.
func (NoopTask) Run(context.Context) {}

// Task is a typed task submitted through Submit.
type Task[T any] interface {
	Execute(ctx context.Context) (T, error)
}

// TaskFunc adapts a named function value to Task.
type TaskFunc[T any] func(ctx context.Context) (T, error)

// Execute calls the wrapped function.
func (fn TaskFunc[T]) Execute(ctx context.Context) (T, error) {
	if fn == nil {
		var zero T

		return zero, fmt.Errorf("%w: task func is nil", ErrInvalid)
	}

	return fn(ctx)
}

// SubmitOption configures typed submissions.
type SubmitOption func(config *SubmitConfig) error

// SubmitConfig is the validated typed submission configuration.
type SubmitConfig struct {
	Name string
}

// WithTaskName sets the submitted task name used by metrics and diagnostics.
func WithTaskName(name string) SubmitOption {
	return func(config *SubmitConfig) error {
		name = strings.TrimSpace(name)
		if name == "" {
			return fmt.Errorf("%w: task name is empty", ErrInvalid)
		}

		config.Name = name
		return nil
	}
}
