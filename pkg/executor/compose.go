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
	"errors"
	"fmt"
	"sync"
)

// ApplyTask maps a parent future value to a new value.
type ApplyTask[T, R any] interface {
	Apply(ctx context.Context, value T) (R, error)
}

// ApplyTaskFunc adapts a named function value to ApplyTask.
type ApplyTaskFunc[T, R any] func(ctx context.Context, value T) (R, error)

// Apply calls the wrapped function.
func (fn ApplyTaskFunc[T, R]) Apply(ctx context.Context, value T) (R, error) {
	if fn == nil {
		var zero R

		return zero, fmt.Errorf("%w: apply task func is nil", ErrInvalid)
	}

	return fn(ctx, value)
}

// ComposeTask maps a parent future value to another future.
type ComposeTask[T, R any] interface {
	Compose(ctx context.Context, value T) (Future[R], error)
}

// ComposeTaskFunc adapts a named function value to ComposeTask.
type ComposeTaskFunc[T, R any] func(ctx context.Context, value T) (Future[R], error)

// Compose calls the wrapped function.
func (fn ComposeTaskFunc[T, R]) Compose(ctx context.Context, value T) (Future[R], error) {
	if fn == nil {
		return nil, fmt.Errorf("%w: compose task func is nil", ErrInvalid)
	}

	return fn(ctx, value)
}

// RecoverTask maps a parent future error to a replacement value.
type RecoverTask[T any] interface {
	Recover(ctx context.Context, err error) (T, error)
}

// RecoverTaskFunc adapts a named function value to RecoverTask.
type RecoverTaskFunc[T any] func(ctx context.Context, err error) (T, error)

// Recover calls the wrapped function.
func (fn RecoverTaskFunc[T]) Recover(ctx context.Context, err error) (T, error) {
	if fn == nil {
		var zero T

		return zero, fmt.Errorf("%w: recover task func is nil", ErrInvalid)
	}

	return fn(ctx, err)
}

// AllOf completes when every future completes successfully.
func AllOf(futures ...FutureView) Future[struct{}] {
	promise := NewPromise[struct{}]()
	if len(futures) == 0 {
		promise.Complete(struct{}{})
		return promise.Future()
	}

	active := make([]FutureView, 0, len(futures))
	var joined error
	for _, future := range futures {
		if future == nil {
			joined = errors.Join(joined, fmt.Errorf("%w: future is nil", ErrInvalid))
			continue
		}
		active = append(active, future)
	}
	if len(active) == 0 {
		promise.Fail(joined)
		return promise.Future()
	}

	var mu sync.Mutex
	remaining := len(active)
	var canceled bool
	for _, future := range active {
		future.ObserveAny(FutureObserverFunc[any](func(result Result[any]) {
			mu.Lock()
			defer mu.Unlock()
			if result.Err != nil {
				joined = errors.Join(joined, result.Err)
			}
			if result.Canceled {
				canceled = true
			}
			remaining--
			completeAllOf(promise, remaining, joined, canceled)
		}))
	}

	return promise.Future()
}

func completeAllOf(
	promise Promise[struct{}],
	remaining int,
	err error,
	canceled bool,
) {
	if remaining > 0 {
		return
	}
	if err != nil {
		if canceled {
			promise.Cancel(err)
			return
		}
		promise.Fail(err)
		return
	}
	promise.Complete(struct{}{})
}

// All completes with all values in input order when every future succeeds.
func All[T any](futures ...Future[T]) Future[[]T] {
	promise := NewPromise[[]T]()
	if len(futures) == 0 {
		promise.Complete([]T{})
		return promise.Future()
	}

	active := make([]Future[T], 0, len(futures))
	indexes := make([]int, 0, len(futures))
	values := make([]T, len(futures))
	var joined error
	for index, future := range futures {
		if future == nil {
			joined = errors.Join(joined, fmt.Errorf("%w: future is nil", ErrInvalid))
			continue
		}
		active = append(active, future)
		indexes = append(indexes, index)
	}
	if len(active) == 0 {
		promise.Fail(joined)
		return promise.Future()
	}

	var mu sync.Mutex
	remaining := len(active)
	var canceled bool
	for activeIndex, future := range active {
		resultIndex := indexes[activeIndex]
		future.Observe(FutureObserverFunc[T](func(result Result[T]) {
			mu.Lock()
			defer mu.Unlock()
			if result.OK() {
				values[resultIndex] = result.Value
			}
			if result.Err != nil {
				joined = errors.Join(joined, result.Err)
			}
			if result.Canceled {
				canceled = true
			}
			remaining--
			if remaining > 0 {
				return
			}
			if joined != nil {
				if canceled {
					promise.Cancel(joined)
					return
				}
				promise.Fail(joined)
				return
			}
			promise.Complete(values)
		}))
	}

	return promise.Future()
}

// AnyOf completes with the first successful future result.
func AnyOf(futures ...FutureView) Future[any] {
	promise := NewPromise[any]()
	if len(futures) == 0 {
		promise.Fail(fmt.Errorf("%w: no futures", ErrInvalid))
		return promise.Future()
	}

	active := make([]FutureView, 0, len(futures))
	var joined error
	for _, future := range futures {
		if future == nil {
			joined = errors.Join(joined, fmt.Errorf("%w: future is nil", ErrInvalid))
			continue
		}
		active = append(active, future)
	}
	if len(active) == 0 {
		promise.Fail(joined)
		return promise.Future()
	}

	var mu sync.Mutex
	remaining := len(active)
	for _, future := range active {
		future.ObserveAny(FutureObserverFunc[any](func(result Result[any]) {
			mu.Lock()
			defer mu.Unlock()
			if promiseCompleted(promise.Future()) {
				return
			}
			if result.OK() {
				promise.Complete(result.Value)
				return
			}
			joined = errors.Join(joined, result.Err)
			remaining--
			if remaining == 0 {
				promise.Fail(joined)
			}
		}))
	}

	return promise.Future()
}

// Any completes with the first successful future result.
func Any[T any](futures ...Future[T]) Future[T] {
	promise := NewPromise[T]()
	if len(futures) == 0 {
		promise.Fail(fmt.Errorf("%w: no futures", ErrInvalid))
		return promise.Future()
	}

	active := make([]Future[T], 0, len(futures))
	var joined error
	for _, future := range futures {
		if future == nil {
			joined = errors.Join(joined, fmt.Errorf("%w: future is nil", ErrInvalid))
			continue
		}
		active = append(active, future)
	}
	if len(active) == 0 {
		promise.Fail(joined)
		return promise.Future()
	}

	var mu sync.Mutex
	remaining := len(active)
	for _, future := range active {
		future.Observe(FutureObserverFunc[T](func(result Result[T]) {
			mu.Lock()
			defer mu.Unlock()
			if promiseCompleted(promise.Future()) {
				return
			}
			if result.OK() {
				promise.Complete(result.Value)
				return
			}
			joined = errors.Join(joined, result.Err)
			remaining--
			if remaining == 0 {
				promise.Fail(joined)
			}
		}))
	}

	return promise.Future()
}

// ThenApply runs task on executor after parent completes successfully.
func ThenApply[T, R any](
	ctx context.Context,
	executor Executor,
	parent Future[T],
	task ApplyTask[T, R],
	opts ...SubmitOption,
) (Future[R], error) {
	if parent == nil {
		return nil, fmt.Errorf("%w: parent future is nil", ErrInvalid)
	}

	promise := NewPromise[R]()
	parent.Observe(FutureObserverFunc[T](func(result Result[T]) {
		if !result.OK() {
			completeFromResult(promise, result)
			return
		}
		future, err := Submit(
			ctx,
			executor,
			TaskFunc[R](func(ctx context.Context) (R, error) {
				return task.Apply(ctx, result.Value)
			}),
			opts...,
		)
		chainFuture(promise, future, err)
	}))

	return promise.Future(), nil
}

// ThenCompose runs task on executor after parent succeeds and flattens its future.
func ThenCompose[T, R any](
	ctx context.Context,
	executor Executor,
	parent Future[T],
	task ComposeTask[T, R],
	opts ...SubmitOption,
) (Future[R], error) {
	if parent == nil {
		return nil, fmt.Errorf("%w: parent future is nil", ErrInvalid)
	}

	promise := NewPromise[R]()
	parent.Observe(FutureObserverFunc[T](func(result Result[T]) {
		if !result.OK() {
			completeFromResult(promise, result)
			return
		}
		future, err := Submit(
			ctx,
			executor,
			TaskFunc[Future[R]](func(ctx context.Context) (Future[R], error) {
				return task.Compose(ctx, result.Value)
			}),
			opts...,
		)
		if err != nil {
			promise.Fail(err)
			return
		}
		future.Observe(FutureObserverFunc[Future[R]](func(result Result[Future[R]]) {
			if !result.OK() {
				completeFromResult(promise, result)
				return
			}
			chainFuture(promise, result.Value, nil)
		}))
	}))

	return promise.Future(), nil
}

// Exceptionally runs task on executor after parent fails.
func Exceptionally[T any](
	ctx context.Context,
	executor Executor,
	parent Future[T],
	task RecoverTask[T],
	opts ...SubmitOption,
) (Future[T], error) {
	if parent == nil {
		return nil, fmt.Errorf("%w: parent future is nil", ErrInvalid)
	}

	promise := NewPromise[T]()
	parent.Observe(FutureObserverFunc[T](func(result Result[T]) {
		if result.OK() {
			promise.Complete(result.Value)
			return
		}
		future, err := Submit(
			ctx,
			executor,
			TaskFunc[T](func(ctx context.Context) (T, error) {
				return task.Recover(ctx, result.Err)
			}),
			opts...,
		)
		chainFuture(promise, future, err)
	}))

	return promise.Future(), nil
}

func chainFuture[T any](promise Promise[T], future Future[T], err error) {
	if err != nil {
		promise.Fail(err)
		return
	}
	if future == nil {
		promise.Fail(fmt.Errorf("%w: future is nil", ErrInvalid))
		return
	}
	future.Observe(FutureObserverFunc[T](func(result Result[T]) {
		if result.OK() {
			promise.Complete(result.Value)
			return
		}
		if result.Canceled {
			promise.Cancel(result.Err)
			return
		}
		promise.Fail(result.Err)
	}))
}

func completeFromResult[T, R any](promise Promise[R], result Result[T]) {
	if result.Canceled {
		promise.Cancel(result.Err)
		return
	}
	promise.Fail(result.Err)
}

func promiseCompleted[T any](future Future[T]) bool {
	_, ok := future.Result()

	return ok
}
