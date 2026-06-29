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
		future.ObserveAny(allOfObserver{
			mu:        &mu,
			remaining: &remaining,
			joined:    &joined,
			canceled:  &canceled,
			promise:   promise,
		})
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
		future.Observe(allObserver[T]{
			mu:          &mu,
			remaining:   &remaining,
			joined:      &joined,
			canceled:    &canceled,
			values:      values,
			resultIndex: resultIndex,
			promise:     promise,
		})
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
		future.ObserveAny(anyOfObserver{
			mu:        &mu,
			remaining: &remaining,
			joined:    &joined,
			promise:   promise,
		})
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
		future.Observe(anyObserver[T]{
			mu:        &mu,
			remaining: &remaining,
			joined:    &joined,
			promise:   promise,
		})
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
	parent.Observe(thenApplyObserver[T, R]{
		ctx:      ctx,
		executor: executor,
		task:     task,
		opts:     opts,
		promise:  promise,
	})

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
	parent.Observe(thenComposeObserver[T, R]{
		ctx:      ctx,
		executor: executor,
		task:     task,
		opts:     opts,
		promise:  promise,
	})

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
	parent.Observe(exceptionallyObserver[T]{
		ctx:      ctx,
		executor: executor,
		task:     task,
		opts:     opts,
		promise:  promise,
	})

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
	future.Observe(chainFutureObserver[T]{promise: promise})
}

type allOfObserver struct {
	mu        *sync.Mutex
	remaining *int
	joined    *error
	canceled  *bool
	promise   Promise[struct{}]
}

func (observer allOfObserver) OnFutureComplete(result Result[any]) {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	if result.Canceled {
		*observer.canceled = true
	}
	if err := resultError(result); err != nil {
		*observer.joined = errors.Join(*observer.joined, err)
	}
	*observer.remaining--
	completeAllOf(
		observer.promise,
		*observer.remaining,
		*observer.joined,
		*observer.canceled,
	)
}

type allObserver[T any] struct {
	mu          *sync.Mutex
	remaining   *int
	joined      *error
	canceled    *bool
	values      []T
	resultIndex int
	promise     Promise[[]T]
}

func (observer allObserver[T]) OnFutureComplete(result Result[T]) {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	if result.OK() {
		observer.values[observer.resultIndex] = result.Value
	}
	if result.Canceled {
		*observer.canceled = true
	}
	if err := resultError(result); err != nil {
		*observer.joined = errors.Join(*observer.joined, err)
	}
	*observer.remaining--
	if *observer.remaining > 0 {
		return
	}
	if *observer.joined != nil {
		if *observer.canceled {
			observer.promise.Cancel(*observer.joined)
			return
		}
		observer.promise.Fail(*observer.joined)
		return
	}
	observer.promise.Complete(observer.values)
}

type anyOfObserver struct {
	mu        *sync.Mutex
	remaining *int
	joined    *error
	promise   Promise[any]
}

func (observer anyOfObserver) OnFutureComplete(result Result[any]) {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	if promiseCompleted(observer.promise.Future()) {
		return
	}
	if result.OK() {
		observer.promise.Complete(result.Value)
		return
	}
	*observer.joined = errors.Join(*observer.joined, resultError(result))
	*observer.remaining--
	if *observer.remaining == 0 {
		completeAnyOf(observer.promise, *observer.joined)
	}
}

type anyObserver[T any] struct {
	mu        *sync.Mutex
	remaining *int
	joined    *error
	promise   Promise[T]
}

func (observer anyObserver[T]) OnFutureComplete(result Result[T]) {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	if promiseCompleted(observer.promise.Future()) {
		return
	}
	if result.OK() {
		observer.promise.Complete(result.Value)
		return
	}
	*observer.joined = errors.Join(*observer.joined, resultError(result))
	*observer.remaining--
	if *observer.remaining == 0 {
		completeAny(observer.promise, *observer.joined)
	}
}

type thenApplyObserver[T, R any] struct {
	ctx      context.Context
	executor Executor
	task     ApplyTask[T, R]
	opts     []SubmitOption
	promise  Promise[R]
}

func (observer thenApplyObserver[T, R]) OnFutureComplete(result Result[T]) {
	if !result.OK() {
		completeFromResult(observer.promise, result)
		return
	}
	future, err := Submit(
		observer.ctx,
		observer.executor,
		thenApplyTask[T, R]{
			task:  observer.task,
			value: result.Value,
		},
		observer.opts...,
	)
	chainFuture(observer.promise, future, err)
}

type thenApplyTask[T, R any] struct {
	task  ApplyTask[T, R]
	value T
}

func (task thenApplyTask[T, R]) Execute(ctx context.Context) (R, error) {
	return task.task.Apply(ctx, task.value)
}

type thenComposeObserver[T, R any] struct {
	ctx      context.Context
	executor Executor
	task     ComposeTask[T, R]
	opts     []SubmitOption
	promise  Promise[R]
}

func (observer thenComposeObserver[T, R]) OnFutureComplete(result Result[T]) {
	if !result.OK() {
		completeFromResult(observer.promise, result)
		return
	}
	future, err := Submit(
		observer.ctx,
		observer.executor,
		thenComposeTask[T, R]{
			task:  observer.task,
			value: result.Value,
		},
		observer.opts...,
	)
	if err != nil {
		observer.promise.Fail(err)
		return
	}
	future.Observe(thenComposeFutureObserver[R]{promise: observer.promise})
}

type thenComposeTask[T, R any] struct {
	task  ComposeTask[T, R]
	value T
}

func (task thenComposeTask[T, R]) Execute(ctx context.Context) (Future[R], error) {
	return task.task.Compose(ctx, task.value)
}

type thenComposeFutureObserver[T any] struct {
	promise Promise[T]
}

func (observer thenComposeFutureObserver[T]) OnFutureComplete(result Result[Future[T]]) {
	if !result.OK() {
		completeFromResult(observer.promise, result)
		return
	}
	chainFuture(observer.promise, result.Value, nil)
}

type exceptionallyObserver[T any] struct {
	ctx      context.Context
	executor Executor
	task     RecoverTask[T]
	opts     []SubmitOption
	promise  Promise[T]
}

func (observer exceptionallyObserver[T]) OnFutureComplete(result Result[T]) {
	if result.OK() {
		observer.promise.Complete(result.Value)
		return
	}
	future, err := Submit(
		observer.ctx,
		observer.executor,
		recoverTaskExecution[T]{
			task: observer.task,
			err:  resultError(result),
		},
		observer.opts...,
	)
	chainFuture(observer.promise, future, err)
}

type recoverTaskExecution[T any] struct {
	task RecoverTask[T]
	err  error
}

func (task recoverTaskExecution[T]) Execute(ctx context.Context) (T, error) {
	return task.task.Recover(ctx, task.err)
}

type chainFutureObserver[T any] struct {
	promise Promise[T]
}

func (observer chainFutureObserver[T]) OnFutureComplete(result Result[T]) {
	if result.OK() {
		observer.promise.Complete(result.Value)
		return
	}
	if result.Canceled {
		observer.promise.Cancel(resultError(result))
		return
	}
	observer.promise.Fail(resultError(result))
}

func completeFromResult[T, R any](promise Promise[R], result Result[T]) {
	if result.Canceled {
		promise.Cancel(resultError(result))
		return
	}
	promise.Fail(resultError(result))
}

func promiseCompleted[T any](future Future[T]) bool {
	_, ok := future.Result()

	return ok
}

func completeAnyOf(promise Promise[any], err error) {
	if errors.Is(err, ErrCanceled) {
		promise.Cancel(err)
		return
	}
	promise.Fail(err)
}

func completeAny[T any](promise Promise[T], err error) {
	if errors.Is(err, ErrCanceled) {
		promise.Cancel(err)
		return
	}
	promise.Fail(err)
}

func resultError[T any](result Result[T]) error {
	if result.Canceled && result.Err == nil {
		return ErrCanceled
	}
	if result.Err == nil && !result.OK() {
		return ErrInvalid
	}

	return result.Err
}
