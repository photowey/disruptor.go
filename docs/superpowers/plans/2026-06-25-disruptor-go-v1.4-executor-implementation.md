# Disruptor.go V1.4 Executor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Objective:** Add a public `pkg/executor` package with typed Future/Promise
composition and use it to enable optional parallel RuntimeGraph node execution.

**Architecture:** `pkg/executor` owns bounded task execution, typed read-only futures, producer-owned promises, composition helpers, and executor metrics. RuntimeGraph keeps one scheduler as route-state owner and delegates only handler execution to an executor; workers return internal completion envelopes.

**Tech Stack:** Go 1.26, standard library concurrency primitives, `go.uber.org/goleak`, existing Disruptor RuntimeGraph APIs.

---

## File Structure

- Create `pkg/executor/doc.go`: package documentation.
- Create `pkg/executor/errors.go`: stable executor sentinel errors.
- Create `pkg/executor/executor.go`: `Executor`, `SubmitRequest`, `Task`, `TaskFunc`, `RunnableTask`, submit options.
- Create `pkg/executor/future.go`: `Future[T]`, `FutureView`, observer interfaces, result types, subscriptions.
- Create `pkg/executor/promise.go`: concurrency-safe promise implementation.
- Create `pkg/executor/submit.go`: typed `Submit[T]` wrapper.
- Create `pkg/executor/compose.go`: `AllOf`, `All`, `AnyOf`, `Any`, `ThenApply`, `ThenCompose`, `Exceptionally`.
- Create `pkg/executor/inline.go`: inline executor.
- Create `pkg/executor/fixed_worker.go`: bounded fixed worker executor.
- Create `pkg/executor/metrics.go`: metrics and panic handler contracts.
- Create `pkg/executor/example_test.go`: executable examples.
- Create `pkg/executor/*_test.go`: TDD behavior coverage and goleak checks.
- Modify `pkg/disruptor/runtime_graph_processors.go`: runtime graph executor option, internal executor ownership, completion envelope, parallel scheduling.
- Modify `pkg/disruptor/runtime_graph_test.go`: executor integration tests.
- Modify `benchmarks/runtime_graph_benchmark_test.go`: parallel runtime graph benchmark.
- Add `benchmarks/executor_benchmark_test.go`: executor and Future/Promise benchmarks.
- Modify `benchmarks/README.md`: benchmark interpretation.
- Modify `docs/api-guide.md`, `README.md`, `README.zh-CN.md`: API docs.
- Modify `Makefile`: benchmark smoke includes executor benchmarks.

## Task 1: Revise Design And Add Executor RED Tests

- [x] **Step 1: Revise executor specification**

Updated `docs/superpowers/specs/2026-06-25-disruptor-go-v1.4-executor-design.md` with Future ownership, executor lifecycle ownership, RuntimeGraph completion envelope, compatibility notes, failure matrix, and quality gates.

- [ ] **Step 2: Create failing executor package tests**

Create tests in `pkg/executor` for:

- `TestPromiseCompletesExactlyOnce`
- `TestPromiseAwaitRespectsContext`
- `TestFutureObserversRunForLateAndEarlySubscribers`
- `TestSubmitCompletesTypedFuture`
- `TestSubmitRejectsNilExecutorAndTask`
- `TestInlineExecutorRunsTaskImmediately`
- `TestFixedWorkerExecutorRejectsWhenSaturated`
- `TestFixedWorkerExecutorBlockPolicyRespectsContext`
- `TestFixedWorkerExecutorShutdownStopsAcceptingTasks`
- `TestAllAndAnyComposition`
- `TestThenApplyThenComposeAndExceptionally`

- [ ] **Step 3: Run RED executor tests**

Run:

```bash
go test ./pkg/executor -count=1
```

Expected: fail because `pkg/executor` implementation does not exist yet.

## Task 2: Implement pkg/executor

- [ ] **Step 1: Add package documentation and public contracts**

Create `pkg/executor/doc.go`, `errors.go`, `executor.go`, `future.go`, and `metrics.go` with Apache headers and English godoc.

- [ ] **Step 2: Implement promise and future**

Implement a mutex-protected promise with exactly-once completion, late observer notification, non-blocking result reads, and `Await(ctx)`.

- [ ] **Step 3: Implement typed submit and inline executor**

Implement `Submit[T]`, typed runnable wrapper, panic recovery into failed futures, and `InlineExecutor`.

- [ ] **Step 4: Implement composition helpers**

Implement `AllOf`, `All`, `AnyOf`, `Any`, `ThenApply`, `ThenCompose`, and `Exceptionally` without hidden unbounded goroutines or worker-slot waits.

- [ ] **Step 5: Implement fixed worker executor**

Implement bounded fixed workers with block/reject policies, metrics hooks, panic handler notification, graceful shutdown, and context-aware submit.

- [ ] **Step 6: Run GREEN executor tests**

Run:

```bash
go test ./pkg/executor -count=1
go test -race ./pkg/executor -count=1
```

Expected: pass.

## Task 3: Add RuntimeGraph Executor Integration RED Tests

- [ ] **Step 1: Write failing RuntimeGraph tests**

Add tests in `pkg/disruptor/runtime_graph_test.go`:

- `TestRuntimeGraphWorkersExecuteIndependentNodesConcurrently`
- `TestRuntimeGraphRejectsWorkersAndExecutorTogether`
- `TestRuntimeGraphExternalExecutorIsCallerOwned`
- `TestRuntimeGraphExecutorFailureUsesExceptionKindExecutor`
- `TestRuntimeGraphParallelJoinWaitsForInFlightHandlersOnHalt`

- [ ] **Step 2: Run RED RuntimeGraph tests**

Run:

```bash
go test ./pkg/disruptor -run 'TestRuntimeGraph(Workers|RejectsWorkers|ExternalExecutor|ExecutorFailure|ParallelJoin)' -count=1
```

Expected: fail because RuntimeGraph has not yet integrated `pkg/executor`.

## Task 4: Implement RuntimeGraph Executor Integration

- [ ] **Step 1: Add executor option and exception kind**

Modify `pkg/disruptor/runtime_graph_processors.go`:

- add `RuntimeGraphExceptionKindExecutor`.
- add `WithRuntimeGraphExecutor[T](executor executor.Executor)`.
- reject using both `WithRuntimeGraphWorkers` and `WithRuntimeGraphExecutor`.

- [ ] **Step 2: Add executor ownership and lifecycle**

RuntimeGraph owns only internally-created executors. `OnStart` validates worker configuration. `OnShutdown` waits for in-flight tasks and shuts down only internal executors.

- [ ] **Step 3: Add scheduler completion envelope**

Introduce internal `runtimeGraphNodeResult[T]` with node index, request, duration, error, and panic marker.

- [ ] **Step 4: Split node execution from route advancement**

Workers execute only handlers. Scheduler receives completion, handles retry/continue/halt, emits metrics, evaluates outbound edges, resolves joins, and advances END/no-route.

- [ ] **Step 5: Preserve inline default**

Default `workers == 1` uses inline executor and preserves existing deterministic tests.

- [ ] **Step 6: Run GREEN RuntimeGraph tests**

Run:

```bash
go test ./pkg/disruptor -run 'TestRuntimeGraph|TestDisruptorHandleRuntimeGraph' -count=1
go test -race ./pkg/disruptor -run 'TestRuntimeGraph|TestDisruptorHandleRuntimeGraph' -count=1
```

Expected: pass.

## Task 5: Benchmarks, Examples, And Documentation

- [ ] **Step 1: Add executor examples**

Create executable examples in `pkg/executor/example_test.go` without raw anonymous function parameters in public API calls.

- [ ] **Step 2: Add executor benchmarks**

Create `benchmarks/executor_benchmark_test.go` with:

- `BenchmarkFutureAwaitCompleted`
- `BenchmarkPromiseComplete`
- `BenchmarkExecutorSubmitInline`
- `BenchmarkExecutorSubmitFixedWorker`
- `BenchmarkFutureAllOf`

- [ ] **Step 3: Add RuntimeGraph parallel benchmark**

Add `BenchmarkRuntimeGraphRouting/parallel_workers` or a dedicated `BenchmarkRuntimeGraphRoutingParallel`.

- [ ] **Step 4: Update docs**

Update `README.md`, `README.zh-CN.md`, `docs/api-guide.md`, `benchmarks/README.md`, and examples to document executor usage, ownership, cancellation, backpressure, and RuntimeGraph parallel workers.

- [ ] **Step 5: Update Makefile benchmark smoke**

Ensure `make bench-smoke` includes executor benchmarks.

## Task 6: Full Verification And Commit

- [ ] **Step 1: Run formatting and diff checks**

Run:

```bash
go fmt ./...
git diff --check
```

- [ ] **Step 2: Run full test suite**

Run:

```bash
go test ./... -count=1
go test -race ./... -count=1
```

- [ ] **Step 3: Run lint and Makefile CI**

Run:

```bash
golangci-lint run --timeout=10m
make ci
```

- [ ] **Step 4: Run benchmark smoke**

Run:

```bash
go test -run '^$' -bench='Benchmark(Future|Promise|Executor|RuntimeGraph)' -benchmem -count=3 ./...
```

- [ ] **Step 5: Run GitNexus change detection**

Run `gitnexus_detect_changes(scope="all")` and confirm affected symbols match v1.4 executor and RuntimeGraph scope.

- [ ] **Step 6: Commit and push**

Stage only v1.4-related files, leaving unrelated `AGENTS.md` and `CLAUDE.md` changes untouched unless explicitly requested.
