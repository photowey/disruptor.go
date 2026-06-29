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

package benchmarks

import (
	"context"
	"testing"

	"github.com/photowey/disruptor.go/pkg/executor"
)

func BenchmarkFutureAwaitCompleted(b *testing.B) {
	ctx := context.Background()
	future := executor.CompletedFuture(42)

	b.ReportAllocs()
	for b.Loop() {
		value, err := future.Await(ctx)
		if err != nil {
			b.Fatalf("await: %v", err)
		}
		if value != 42 {
			b.Fatalf("value = %d, want 42", value)
		}
	}
}

func BenchmarkPromiseComplete(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		promise := executor.NewPromise[int]()
		if !promise.Complete(42) {
			b.Fatal("complete returned false")
		}
	}
}

func BenchmarkExecutorSubmitInline(b *testing.B) {
	ctx := context.Background()
	inline := executor.NewInlineExecutor()
	task := benchmarkIntTask{value: 42}

	b.ReportAllocs()
	for b.Loop() {
		future, err := executor.Submit(ctx, inline, task)
		if err != nil {
			b.Fatalf("submit: %v", err)
		}
		if _, err := future.Await(ctx); err != nil {
			b.Fatalf("await: %v", err)
		}
	}
}

func BenchmarkExecutorSubmitFixedWorker(b *testing.B) {
	ctx := context.Background()
	pool, err := executor.NewFixedWorkerExecutor(
		executor.WithWorkers(2),
		executor.WithQueueSize(1024),
	)
	if err != nil {
		b.Fatalf("new fixed worker: %v", err)
	}
	b.Cleanup(benchmarkExecutorCleanup{
		b:        b,
		executor: pool,
	}.cleanup)

	task := benchmarkIntTask{value: 42}

	b.ReportAllocs()
	for b.Loop() {
		future, err := executor.Submit(ctx, pool, task)
		if err != nil {
			b.Fatalf("submit: %v", err)
		}
		if _, err := future.Await(ctx); err != nil {
			b.Fatalf("await: %v", err)
		}
	}
}

type benchmarkExecutorCleanup struct {
	b        *testing.B
	executor executor.Executor
}

func (c benchmarkExecutorCleanup) cleanup() {
	if err := c.executor.Shutdown(context.Background()); err != nil {
		c.b.Fatalf("shutdown fixed worker: %v", err)
	}
}

type benchmarkIntTask struct {
	value int
}

func (task benchmarkIntTask) Execute(context.Context) (int, error) {
	return task.value, nil
}

func BenchmarkFutureAllOf(b *testing.B) {
	first := executor.CompletedFuture(1)
	second := executor.CompletedFuture(2)

	b.ReportAllocs()
	for b.Loop() {
		future := executor.All(first, second)
		if _, err := future.Await(context.Background()); err != nil {
			b.Fatalf("await: %v", err)
		}
	}
}
