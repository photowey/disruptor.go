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

// Submit submits a typed task to an executor and returns its typed future.
func Submit[T any](
	ctx context.Context,
	executor Executor,
	task Task[T],
	opts ...SubmitOption,
) (Future[T], error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if executor == nil {
		return nil, fmt.Errorf("%w: executor is nil", ErrInvalid)
	}
	if task == nil {
		return nil, fmt.Errorf("%w: task is nil", ErrInvalid)
	}

	config := SubmitConfig{}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt.applySubmit(&config); err != nil {
			return nil, fmt.Errorf("applying submit option: %w", err)
		}
	}

	promise := NewPromise[T]()
	runnable := typedRunnableTask[T]{
		task:    task,
		promise: promise,
	}
	if err := executor.Submit(SubmitRequest{
		Context: ctx,
		Task:    runnable,
		Name:    config.Name,
	}); err != nil {
		return nil, err
	}

	return promise.Future(), nil
}

type typedRunnableTask[T any] struct {
	task    Task[T]
	promise Promise[T]
}

func (t typedRunnableTask[T]) Cancel(cause error) {
	t.promise.Cancel(cause)
}

func (t typedRunnableTask[T]) Run(ctx context.Context) {
	defer func() {
		if recovered := recover(); recovered != nil {
			t.promise.Fail(fmt.Errorf("executor: task panic: %v", recovered))
		}
	}()

	value, err := t.task.Execute(ctx)
	if err != nil {
		t.promise.Fail(err)
		return
	}

	t.promise.Complete(value)
}
