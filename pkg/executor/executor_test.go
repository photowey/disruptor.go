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

package executor_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/photowey/disruptor.go/pkg/executor"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestPromiseCompletesExactlyOnce(t *testing.T) {
	promise := executor.NewPromise[int]()

	const attempts = 32
	var wg sync.WaitGroup
	wg.Add(attempts)
	for index := 0; index < attempts; index++ {
		value := index
		go func() {
			defer wg.Done()
			_ = promise.Complete(value)
		}()
	}
	wg.Wait()

	result, ok := promise.Future().Result()
	if !ok {
		t.Fatal("future is not complete")
	}
	if !result.OK() {
		t.Fatalf("result OK = false, err=%v canceled=%v", result.Err, result.Canceled)
	}
	if promise.Complete(100) {
		t.Fatal("second complete succeeded")
	}
	if promise.Fail(errors.New("late failure")) {
		t.Fatal("late failure succeeded")
	}
}

func TestPromiseAwaitRespectsContext(t *testing.T) {
	promise := executor.NewPromise[int]()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := promise.Future().Await(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("await error = %v, want context.Canceled", err)
	}
}

func TestFutureObserversRunForLateAndEarlySubscribers(t *testing.T) {
	promise := executor.NewPromise[int]()
	early := make(chan int, 1)
	late := make(chan int, 1)

	promise.Future().Observe(executor.FutureObserverFunc[int](func(result executor.Result[int]) {
		early <- result.Value
	}))
	if !promise.Complete(42) {
		t.Fatal("complete returned false")
	}
	promise.Future().Observe(executor.FutureObserverFunc[int](func(result executor.Result[int]) {
		late <- result.Value
	}))

	wantObserverValue(t, early, 42)
	wantObserverValue(t, late, 42)
}

func TestSubmitCompletesTypedFuture(t *testing.T) {
	ctx := context.Background()
	inline := executor.NewInlineExecutor()
	task := executor.TaskFunc[int](func(context.Context) (int, error) {
		return 7, nil
	})

	future, err := executor.Submit(ctx, inline, task, executor.WithTaskName("seven"))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	value, err := future.Await(ctx)
	if err != nil {
		t.Fatalf("await: %v", err)
	}
	if value != 7 {
		t.Fatalf("value = %d, want 7", value)
	}
}

func TestSubmitRejectsNilExecutorAndTask(t *testing.T) {
	ctx := context.Background()
	if _, err := executor.Submit[int](ctx, nil, executor.TaskFunc[int](func(context.Context) (int, error) {
		return 0, nil
	})); !errors.Is(err, executor.ErrInvalid) {
		t.Fatalf("nil executor error = %v, want ErrInvalid", err)
	}

	inline := executor.NewInlineExecutor()
	if _, err := executor.Submit[int](ctx, inline, nil); !errors.Is(err, executor.ErrInvalid) {
		t.Fatalf("nil task error = %v, want ErrInvalid", err)
	}
}

func TestInlineExecutorRunsTaskImmediately(t *testing.T) {
	inline := executor.NewInlineExecutor()
	ran := false
	err := inline.Submit(executor.SubmitRequest{
		Context: context.Background(),
		Task: executor.RunnableTaskFunc(func(context.Context) {
			ran = true
		}),
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if !ran {
		t.Fatal("task did not run")
	}
}

func TestFixedWorkerExecutorRejectsWhenSaturated(t *testing.T) {
	pool, err := executor.NewFixedWorkerExecutor(
		executor.WithWorkers(1),
		executor.WithQueueSize(1),
		executor.WithRejectPolicy(executor.RejectPolicyReject),
	)
	if err != nil {
		t.Fatalf("new fixed worker: %v", err)
	}
	defer shutdownExecutor(t, pool)

	release := make(chan struct{})
	started := make(chan struct{})
	blocking := executor.RunnableTaskFunc(func(context.Context) {
		close(started)
		<-release
	})
	if err := pool.Submit(executor.SubmitRequest{Context: context.Background(), Task: blocking}); err != nil {
		t.Fatalf("submit blocking: %v", err)
	}
	<-started
	if err := pool.Submit(executor.SubmitRequest{Context: context.Background(), Task: executor.NoopTask{}}); err != nil {
		t.Fatalf("submit queued: %v", err)
	}
	if err := pool.Submit(executor.SubmitRequest{Context: context.Background(), Task: executor.NoopTask{}}); !errors.Is(err, executor.ErrSaturated) {
		close(release)
		t.Fatalf("saturated submit error = %v, want ErrSaturated", err)
	}
	close(release)
}

func TestFixedWorkerExecutorBlockPolicyRespectsContext(t *testing.T) {
	pool, err := executor.NewFixedWorkerExecutor(
		executor.WithWorkers(1),
		executor.WithQueueSize(1),
		executor.WithRejectPolicy(executor.RejectPolicyBlock),
	)
	if err != nil {
		t.Fatalf("new fixed worker: %v", err)
	}
	defer shutdownExecutor(t, pool)

	release := make(chan struct{})
	started := make(chan struct{})
	if err := pool.Submit(executor.SubmitRequest{
		Context: context.Background(),
		Task: executor.RunnableTaskFunc(func(context.Context) {
			close(started)
			<-release
		}),
	}); err != nil {
		t.Fatalf("submit blocking: %v", err)
	}
	<-started
	if err := pool.Submit(executor.SubmitRequest{Context: context.Background(), Task: executor.NoopTask{}}); err != nil {
		t.Fatalf("submit queued: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := pool.Submit(executor.SubmitRequest{Context: ctx, Task: executor.NoopTask{}}); !errors.Is(err, context.Canceled) {
		close(release)
		t.Fatalf("blocked submit error = %v, want context.Canceled", err)
	}
	close(release)
}

func TestSubmittedFutureCancelsWhenQueuedContextIsCanceled(t *testing.T) {
	ctx := context.Background()
	pool, err := executor.NewFixedWorkerExecutor(
		executor.WithWorkers(1),
		executor.WithQueueSize(1),
		executor.WithRejectPolicy(executor.RejectPolicyReject),
	)
	if err != nil {
		t.Fatalf("new fixed worker: %v", err)
	}
	defer shutdownExecutor(t, pool)

	release := make(chan struct{})
	started := make(chan struct{})
	if err := pool.Submit(executor.SubmitRequest{
		Context: context.Background(),
		Task: executor.RunnableTaskFunc(func(context.Context) {
			close(started)
			<-release
		}),
	}); err != nil {
		t.Fatalf("submit blocking: %v", err)
	}
	<-started

	queuedCtx, cancel := context.WithCancel(context.Background())
	future, err := executor.Submit(
		queuedCtx,
		pool,
		executor.TaskFunc[int](func(context.Context) (int, error) {
			return 42, nil
		}),
	)
	if err != nil {
		close(release)
		t.Fatalf("submit queued typed task: %v", err)
	}
	cancel()
	close(release)

	_, err = future.Await(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("await queued canceled future error = %v, want context.Canceled", err)
	}
}

func TestFixedWorkerExecutorShutdownStopsAcceptingTasks(t *testing.T) {
	pool, err := executor.NewFixedWorkerExecutor(
		executor.WithWorkers(1),
		executor.WithQueueSize(1),
	)
	if err != nil {
		t.Fatalf("new fixed worker: %v", err)
	}
	if err := pool.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if err := pool.Submit(executor.SubmitRequest{Context: context.Background(), Task: executor.NoopTask{}}); !errors.Is(err, executor.ErrClosed) {
		t.Fatalf("submit after shutdown error = %v, want ErrClosed", err)
	}
}

func TestFixedWorkerExecutorShutdownUnblocksPendingSubmit(t *testing.T) {
	pool, err := executor.NewFixedWorkerExecutor(
		executor.WithWorkers(1),
		executor.WithQueueSize(0),
		executor.WithRejectPolicy(executor.RejectPolicyBlock),
	)
	if err != nil {
		t.Fatalf("new fixed worker: %v", err)
	}

	release := make(chan struct{})
	started := make(chan struct{})
	if err := pool.Submit(executor.SubmitRequest{
		Context: context.Background(),
		Task: executor.RunnableTaskFunc(func(context.Context) {
			close(started)
			<-release
		}),
	}); err != nil {
		t.Fatalf("submit blocking: %v", err)
	}
	<-started

	submitErr := make(chan error, 1)
	go submitNoopTask(pool, submitErr)
	waitForPendingSubmit(t, submitErr)

	shutdownErr := make(chan error, 1)
	go shutdownExecutorAsync(pool, shutdownErr)
	if err := <-submitErr; !errors.Is(err, executor.ErrClosed) {
		close(release)
		t.Fatalf("pending submit error = %v, want ErrClosed", err)
	}
	close(release)
	if err := <-shutdownErr; err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

func TestAllAndAnyComposition(t *testing.T) {
	ctx := context.Background()
	first := executor.CompletedFuture(1)
	second := executor.CompletedFuture(2)

	values, err := executor.All(first, second).Await(ctx)
	if err != nil {
		t.Fatalf("all await: %v", err)
	}
	if len(values) != 2 || values[0] != 1 || values[1] != 2 {
		t.Fatalf("values = %v, want [1 2]", values)
	}

	value, err := executor.Any(first, second).Await(ctx)
	if err != nil {
		t.Fatalf("any await: %v", err)
	}
	if value != 1 && value != 2 {
		t.Fatalf("any value = %d, want 1 or 2", value)
	}
}

func TestCompositionHandlesConcurrentCompletion(t *testing.T) {
	ctx := context.Background()

	for attempt := 0; attempt < 100; attempt++ {
		first := executor.NewPromise[int]()
		second := executor.NewPromise[int]()
		start := make(chan struct{})
		go completePromiseAfterStart(start, first, 1)
		go completePromiseAfterStart(start, second, 2)
		close(start)

		values, err := executor.All(first.Future(), second.Future()).Await(ctx)
		if err != nil {
			t.Fatalf("all await attempt %d: %v", attempt, err)
		}
		if len(values) != 2 || values[0] != 1 || values[1] != 2 {
			t.Fatalf("values attempt %d = %v, want [1 2]", attempt, values)
		}

		anyFirst := executor.NewPromise[int]()
		anySecond := executor.NewPromise[int]()
		anyStart := make(chan struct{})
		go completePromiseAfterStart(anyStart, anyFirst, 1)
		go completePromiseAfterStart(anyStart, anySecond, 2)
		close(anyStart)

		value, err := executor.Any(anyFirst.Future(), anySecond.Future()).Await(ctx)
		if err != nil {
			t.Fatalf("any await attempt %d: %v", attempt, err)
		}
		if value != 1 && value != 2 {
			t.Fatalf("any value attempt %d = %d, want 1 or 2", attempt, value)
		}
	}
}

func TestCompositionNormalizesCustomCanceledResults(t *testing.T) {
	ctx := context.Background()
	inline := executor.NewInlineExecutor()
	canceled := completedView[int]{
		result: executor.Result[int]{Canceled: true},
	}
	succeeded := executor.CompletedFuture(7)

	_, err := executor.All(canceled).Await(ctx)
	if !errors.Is(err, executor.ErrCanceled) {
		t.Fatalf("all canceled error = %v, want ErrCanceled", err)
	}

	_, err = executor.AllOf(canceled).Await(ctx)
	if !errors.Is(err, executor.ErrCanceled) {
		t.Fatalf("allOf canceled error = %v, want ErrCanceled", err)
	}

	value, err := executor.Any(canceled, succeeded).Await(ctx)
	if err != nil {
		t.Fatalf("any canceled then succeeded: %v", err)
	}
	if value != 7 {
		t.Fatalf("any value = %d, want 7", value)
	}

	_, err = executor.Any(canceled).Await(ctx)
	if !errors.Is(err, executor.ErrCanceled) {
		t.Fatalf("any canceled error = %v, want ErrCanceled", err)
	}

	_, err = executor.AnyOf(canceled).Await(ctx)
	if !errors.Is(err, executor.ErrCanceled) {
		t.Fatalf("anyOf canceled error = %v, want ErrCanceled", err)
	}

	recovered, err := executor.Exceptionally(
		ctx,
		inline,
		canceled,
		executor.RecoverTaskFunc[int](func(_ context.Context, err error) (int, error) {
			if !errors.Is(err, executor.ErrCanceled) {
				return 0, err
			}

			return 9, nil
		}),
	)
	if err != nil {
		t.Fatalf("exceptionally canceled: %v", err)
	}
	value, err = recovered.Await(ctx)
	if err != nil {
		t.Fatalf("await recovered canceled: %v", err)
	}
	if value != 9 {
		t.Fatalf("recovered canceled value = %d, want 9", value)
	}
}

func TestThenApplyThenComposeAndExceptionally(t *testing.T) {
	ctx := context.Background()
	inline := executor.NewInlineExecutor()
	base := executor.CompletedFuture(10)

	applied, err := executor.ThenApply(
		ctx,
		inline,
		base,
		executor.ApplyTaskFunc[int, int](func(_ context.Context, value int) (int, error) {
			return value + 5, nil
		}),
	)
	if err != nil {
		t.Fatalf("then apply: %v", err)
	}
	value, err := applied.Await(ctx)
	if err != nil {
		t.Fatalf("await applied: %v", err)
	}
	if value != 15 {
		t.Fatalf("applied value = %d, want 15", value)
	}

	composed, err := executor.ThenCompose(
		ctx,
		inline,
		applied,
		executor.ComposeTaskFunc[int, string](func(_ context.Context, value int) (executor.Future[string], error) {
			return executor.CompletedFuture("value"), nil
		}),
	)
	if err != nil {
		t.Fatalf("then compose: %v", err)
	}
	text, err := composed.Await(ctx)
	if err != nil {
		t.Fatalf("await composed: %v", err)
	}
	if text != "value" {
		t.Fatalf("composed value = %q, want value", text)
	}

	failed := executor.FailedFuture[int](errors.New("boom"))
	recovered, err := executor.Exceptionally(
		ctx,
		inline,
		failed,
		executor.RecoverTaskFunc[int](func(context.Context, error) (int, error) {
			return 99, nil
		}),
	)
	if err != nil {
		t.Fatalf("exceptionally: %v", err)
	}
	got, err := recovered.Await(ctx)
	if err != nil {
		t.Fatalf("await recovered: %v", err)
	}
	if got != 99 {
		t.Fatalf("recovered = %d, want 99", got)
	}
}

func wantObserverValue(t *testing.T, values <-chan int, want int) {
	t.Helper()

	select {
	case got := <-values:
		if got != want {
			t.Fatalf("observer value = %d, want %d", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for observer")
	}
}

func shutdownExecutor(t *testing.T, pool executor.Executor) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := pool.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown executor: %v", err)
	}
}

func submitNoopTask(exec executor.Executor, submitErr chan<- error) {
	submitErr <- exec.Submit(executor.SubmitRequest{
		Context: context.Background(),
		Task:    executor.NoopTask{},
	})
}

func shutdownExecutorAsync(exec executor.Executor, shutdownErr chan<- error) {
	shutdownErr <- exec.Shutdown(context.Background())
}

func waitForPendingSubmit(t *testing.T, submitErr <-chan error) {
	t.Helper()

	select {
	case err := <-submitErr:
		t.Fatalf("submit returned before shutdown: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
}

func completePromiseAfterStart[T any](
	start <-chan struct{},
	promise executor.Promise[T],
	value T,
) {
	<-start
	promise.Complete(value)
}

type completedView[T any] struct {
	result executor.Result[T]
	done   chan struct{}
}

func (f completedView[T]) Await(context.Context) (T, error) {
	if f.result.OK() {
		return f.result.Value, nil
	}
	if f.result.Canceled {
		var zero T

		if f.result.Err != nil {
			return zero, f.result.Err
		}

		return zero, executor.ErrCanceled
	}
	var zero T

	return zero, f.result.Err
}

func (f completedView[T]) Done() <-chan struct{} {
	if f.done != nil {
		return f.done
	}

	done := make(chan struct{})
	close(done)

	return done
}

func (f completedView[T]) Result() (executor.Result[T], bool) {
	return f.result, true
}

func (f completedView[T]) Observe(observer executor.FutureObserver[T]) executor.Subscription {
	if observer != nil {
		observer.OnFutureComplete(f.result)
	}

	return completedSubscription{}
}

func (f completedView[T]) ResultAny() (executor.Result[any], bool) {
	return executor.Result[any]{
		Value:    f.result.Value,
		Err:      f.result.Err,
		Canceled: f.result.Canceled,
	}, true
}

func (f completedView[T]) ObserveAny(
	observer executor.FutureObserver[any],
) executor.Subscription {
	if observer != nil {
		result, _ := f.ResultAny()
		observer.OnFutureComplete(result)
	}

	return completedSubscription{}
}

type completedSubscription struct{}

func (completedSubscription) Unsubscribe() bool {
	return false
}
