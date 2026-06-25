# Disruptor.go V1.4 Executor And Future Design

## Status

Design revised after BMAD Party Mode review.

Target tag: `v1.4.0`

This design adds a public `pkg/executor` package for bounded task execution,
typed futures, promises, and CompletableFuture-style composition. RuntimeGraph
will use this package as its first high-performance consumer, but the executor
package is a stable general-purpose API.

## Goals

- Expose a stable public `pkg/executor` package.
- Hide low-level `sync.WaitGroup`, channel fan-in, and worker lifecycle details
  behind a small task and future API.
- Support typed read-only `Future[T]` and producer-owned `Promise[T]`.
- Support composition helpers similar to Java `CompletableFuture`, adapted to
  Go generics and context cancellation.
- Keep public APIs interface-first and replaceable.
- Avoid naked anonymous function parameters in public APIs. Convenience function
  adapters may exist as named `TaskFunc`, `ApplyTaskFunc`, and similar types.
- Provide a bounded fixed worker executor with explicit backpressure behavior.
- Let RuntimeGraph execute independent ready nodes concurrently without letting
  workers mutate scheduler route state.
- Preserve deterministic RuntimeGraph behavior by default.

## Non-Goals

- Do not copy Java's thread-pool API directly. Go already has a goroutine
  scheduler; this package controls task bounds, lifecycle, and composition.
- Do not add dynamic pool resizing in V1.4.0.
- Do not add work stealing in V1.4.0.
- Do not add unbounded queues.
- Do not add preemptive task cancellation. Running tasks must cooperate with
  `context.Context`.
- Do not change RingBuffer, Sequencer, WaitStrategy, or static Graph execution.
- Do not make RuntimeGraph parallel execution the default.

## Design Summary

The design has two layers:

1. `pkg/executor` provides a general bounded executor, futures, promises, and
   composition helpers.
2. `pkg/disruptor` adapts RuntimeGraph scheduling to the executor package.

The key RuntimeGraph invariant is:

```text
scheduler owns state, executor owns execution
```

The scheduler owns route state, edge evaluation, joins, END/no-route decisions,
exception policy, and sequence completion. The executor only runs node handler
tasks. A worker never mutates RuntimeGraph route state directly. Workers report
completion back to the scheduler, and the scheduler advances the state machine.

## Go API Shape

Go does not support generic methods. Because of that, `Executor` itself is
non-generic and generic behavior is provided by package-level helper functions.

This keeps custom executors easy to implement while preserving typed user code:

```go
future, err := executor.Submit[OrderResult](ctx, pool, task)
```

The executor only knows how to run a `RunnableTask`. The typed `Submit[T]`
helper wraps a `Task[T]` into a runnable task and completes a `Promise[T]`.

## Public Executor API Sketch

```go
package executor

type Executor interface {
    Submit(request SubmitRequest) error
    Shutdown(ctx context.Context) error
}

type SubmitRequest struct {
    Context context.Context
    Task    RunnableTask
    Name    string
}

type SubmitOption interface {
    applySubmit(config *SubmitConfig) error
}

type SubmitConfig struct {
    Name string
}

func WithTaskName(name string) SubmitOption

type RunnableTask interface {
    Run(ctx context.Context)
}

type Task[T any] interface {
    Execute(ctx context.Context) (T, error)
}

type TaskFunc[T any] func(ctx context.Context) (T, error)
```

`TaskFunc[T]` is a named adapter type. It is acceptable as a convenience because
the public API accepts `Task[T]`, not a raw anonymous function parameter.

```go
func (fn TaskFunc[T]) Execute(ctx context.Context) (T, error)
```

Typed submission:

```go
func Submit[T any](
    ctx context.Context,
    executor Executor,
    task Task[T],
    opts ...SubmitOption,
) (Future[T], error)
```

`Submit` validates the executor and task, creates a promise, applies submit
options, wraps the typed task in a runnable task, and submits it. If submission
is rejected before the task is accepted, `Submit` returns an error.

## Future And Promise API Sketch

The future API is split into small read-only interfaces and composed into
`Future[T]`. `Future[T]` is an observation API only. Completion and
cancellation belong to `Promise[T]`, not to consumers of a future.

```go
type Awaiter[T any] interface {
    Await(ctx context.Context) (T, error)
    Done() <-chan struct{}
}

type ResultReader[T any] interface {
    Result() (Result[T], bool)
}

type FutureObserver[T any] interface {
    OnFutureComplete(result Result[T])
}

type FutureObserverFunc[T any] func(result Result[T])

type ObservableFuture[T any] interface {
    Observe(observer FutureObserver[T]) Subscription
}

type Subscription interface {
    Unsubscribe() bool
}

type Future[T any] interface {
    Awaiter[T]
    ResultReader[T]
    ObservableFuture[T]
    FutureView
}
```

`FutureView` is the non-generic view used by heterogeneous composition helpers
such as `AllOf`.

```go
type FutureView interface {
    Done() <-chan struct{}
    ResultAny() (Result[any], bool)
    ObserveAny(observer FutureObserver[any]) Subscription
}
```

Result state:

```go
type Result[T any] struct {
    Value    T
    Err      error
    Canceled bool
}

func (r Result[T]) OK() bool
```

Promise:

```go
type Promise[T any] interface {
    Future() Future[T]
    Complete(value T) bool
    Fail(err error) bool
    Cancel(cause error) bool
}

func NewPromise[T any]() Promise[T]
func CompletedFuture[T any](value T) Future[T]
func FailedFuture[T any](err error) Future[T]
func CanceledFuture[T any](cause error) Future[T]
```

Promises are concurrency-safe. Completion is exactly-once. Late observers are
notified immediately with the already completed result.

`Future[T]` intentionally does not expose `Cancel`. This preserves producer
ownership: consumers can wait, observe, or inspect, but cannot complete someone
else's result. Callers cancel submitted work by canceling the submission context
or by owning the promise that backs a future.

## Composition API Sketch

Composition helpers must not create hidden unbounded goroutines and must not
consume worker slots just to wait for other futures. They are implemented by
observing source futures and completing a downstream promise.

```go
func AllOf(futures ...FutureView) Future[struct{}]

func All[T any](futures ...Future[T]) Future[[]T]

func AnyOf(futures ...FutureView) Future[any]

func Any[T any](futures ...Future[T]) Future[T]
```

Continuation helpers schedule real continuation work on an explicit executor:

```go
type ApplyTask[T, R any] interface {
    Apply(ctx context.Context, value T) (R, error)
}

type ApplyTaskFunc[T, R any] func(ctx context.Context, value T) (R, error)

type ComposeTask[T, R any] interface {
    Compose(ctx context.Context, value T) (Future[R], error)
}

type ComposeTaskFunc[T, R any] func(ctx context.Context, value T) (Future[R], error)

type RecoverTask[T any] interface {
    Recover(ctx context.Context, err error) (T, error)
}

type RecoverTaskFunc[T any] func(ctx context.Context, err error) (T, error)
```

```go
func ThenApply[T, R any](
    ctx context.Context,
    executor Executor,
    parent Future[T],
    task ApplyTask[T, R],
    opts ...SubmitOption,
) (Future[R], error)

func ThenCompose[T, R any](
    ctx context.Context,
    executor Executor,
    parent Future[T],
    task ComposeTask[T, R],
    opts ...SubmitOption,
) (Future[R], error)

func Exceptionally[T any](
    ctx context.Context,
    executor Executor,
    parent Future[T],
    task RecoverTask[T],
    opts ...SubmitOption,
) (Future[T], error)
```

Continuations do not run in the goroutine that completes the parent future.
They are submitted to the executor. If continuation submission fails, the child
future completes with that submission error.

## Built-In Executors

V1.4.0 provides two built-in executors:

```go
func NewInlineExecutor() *InlineExecutor

func NewFixedWorkerExecutor(
    opts ...FixedWorkerOption,
) (*FixedWorkerExecutor, error)
```

`InlineExecutor` runs the submitted runnable task immediately. It is useful for
tests, deterministic behavior, and RuntimeGraph defaults.

`FixedWorkerExecutor` starts a fixed number of workers and uses a bounded queue.

```go
type FixedWorkerOption interface {
    applyFixedWorker(config *FixedWorkerConfig) error
}

type FixedWorkerConfig struct {
    Workers       int
    QueueSize     int
    Name          string
    RejectPolicy  RejectPolicy
    PanicHandler  PanicHandler
    MetricsSink   MetricsSink
}

func WithWorkers(workers int) FixedWorkerOption
func WithQueueSize(size int) FixedWorkerOption
func WithName(name string) FixedWorkerOption
func WithRejectPolicy(policy RejectPolicy) FixedWorkerOption
func WithPanicHandler(handler PanicHandler) FixedWorkerOption
func WithMetricsSink(sink MetricsSink) FixedWorkerOption
```

Backpressure policy:

```go
type RejectPolicy uint8

const (
    RejectPolicyBlock RejectPolicy = iota + 1
    RejectPolicyReject
)
```

- `RejectPolicyBlock` waits for queue capacity while observing the submission
  context.
- `RejectPolicyReject` returns `ErrSaturated` immediately when the queue is
  full.

`CallerRuns` is intentionally excluded from V1.4.0. It is useful in some
application pools but unsafe as a RuntimeGraph default because it can make the
scheduler execute node handlers inline.

## Error Model

The package defines stable sentinel errors:

```go
var ErrClosed = errors.New("executor: closed")
var ErrSaturated = errors.New("executor: saturated")
var ErrCanceled = errors.New("executor: canceled")
var ErrInvalid = errors.New("executor: invalid")
```

Task errors complete the future with that error. Panics are recovered by the
typed runnable wrapper and complete the future with an error. The panic handler
receives a notification for observability but does not decide task recovery.

```go
type PanicHandler interface {
    HandleExecutorPanic(request PanicRequest)
}

type PanicHandlerFunc func(request PanicRequest)

type PanicRequest struct {
    Context      context.Context
    ExecutorName string
    TaskName     string
    Recovered    any
}
```

RuntimeGraph continues to use its own exception handler to decide whether a
handler failure halts, retries, or continues.

## Cancellation And Shutdown

Cancellation is cooperative:

- A canceled submission context rejects a task before it is queued.
- A queued task checks the context before execution.
- A running task receives the context and must return when it wants to stop.
- `Promise.Cancel(cause)` completes the promise if it has not already completed.
  It does not forcibly stop a running goroutine.

Shutdown is graceful:

- The executor stops accepting new tasks.
- Accepted tasks are allowed to finish.
- `Shutdown(ctx)` waits until workers exit or the context is canceled.
- If the context expires, `Shutdown` returns the context error. It still cannot
  kill already running goroutines.

## RuntimeGraph Integration

RuntimeGraph adds:

```go
func WithRuntimeGraphExecutor[T any](
    executor executor.Executor,
) RuntimeGraphHandleOption[T]
```

`WithRuntimeGraphWorkers[T](workers int)` remains supported:

- `workers == 1` uses direct inline handler invocation and bypasses executor
  submission to preserve the default zero-allocation RuntimeGraph path.
- `workers > 1` creates an internal fixed worker executor with:
  - `WithWorkers(workers)`
  - `WithQueueSize(workers)`
  - `WithRejectPolicy(executor.RejectPolicyReject)`
  - a RuntimeGraph panic path that reports `RuntimeGraphExceptionKindPanic`
- Supplying both `WithRuntimeGraphWorkers` and `WithRuntimeGraphExecutor` is
  invalid to avoid ambiguous ownership and lifecycle.

Executor ownership:

- RuntimeGraph owns and shuts down executors created from
  `WithRuntimeGraphWorkers`.
- Executors supplied through `WithRuntimeGraphExecutor` are caller-owned.
  Disruptor never calls `Shutdown` on caller-owned executors.
- RuntimeGraph waits for in-flight tasks before returning a handler, condition,
  or executor error from the current event. `OnShutdown` shuts down only an
  internally-owned executor.

RuntimeGraph scheduler behavior:

1. Evaluate START edges.
2. Push ready real nodes into the scheduler ready queue.
3. Submit ready nodes to the executor.
4. Receive worker completions.
5. Apply node result to route state.
6. Evaluate outbound edges.
7. Resolve joins and submit newly ready nodes.
8. Complete the sequence only after END is reached or no-route policy completes.

Workers never evaluate outbound edges and never mutate route state directly.

Worker completion is reported through an internal scheduler-owned message:

```go
type runtimeGraphNodeResult[T any] struct {
    index    int
    request  event.Request[T]
    duration time.Duration
    err      error
    panicked bool
}
```

The completion message contains the node identity, request metadata, duration,
handler error, and panic marker only. Edge evaluation, join resolution,
END/no-route decisions, retry handling, and route-state mutation remain
scheduler-only.

### Scheduler Non-Blocking Rule

RuntimeGraph must guarantee that handler execution does not occupy the
scheduler. The scheduler may wait for completion events because a sequence
cannot advance until the runtime graph reaches END, but the scheduler must not
run handler code when an external executor is configured.

Submission backpressure is separate from handler execution. If submission blocks
under `RejectPolicyBlock`, it must be bounded by context cancellation. For
RuntimeGraph, `RejectPolicyReject` is recommended when saturation should be
reported immediately.

Executor submission failures are reported through RuntimeGraph exception
handling as an executor failure. V1.4.0 adds:

```go
RuntimeGraphExceptionKindExecutor
```

## RuntimeGraph Failure Semantics

RuntimeGraph preserves existing handler semantics:

- successful node: mark done, emit metrics, evaluate outgoing edges.
- handler error + continue: mark done, emit metrics, evaluate outgoing edges.
- handler error + retry: resubmit the same node.
- handler error + halt: stop scheduling new nodes, cancel the per-event graph
  context, wait for in-flight node tasks to finish, and return the error.
- panic: report as panic kind and apply the exception handler decision.

When halting, the scheduler waits for in-flight tasks before returning so the
ring-buffer event and runtime context are not reused while handlers still hold
references to them.

## Compatibility And Concurrency Notes

Default RuntimeGraph execution remains `workers == 1` and deterministic.
`WithRuntimeGraphWorkers(workers > 1)` changes previously accepted configuration
from a forward-compatible hook into active parallel execution.

When `workers > 1` or `WithRuntimeGraphExecutor` is used:

- independent ready nodes may run concurrently.
- independent node completion order is not deterministic.
- handler side effects may be observed in a different order.
- handlers must be concurrency-safe.
- the runtime variable bag is concurrency-safe, but same-key concurrent writes
  use last-write-wins semantics with nondeterministic write order.
- edge evaluation, joins, END, no-route, sequence advancement, and exception
  policy remain deterministic relative to the completion order observed by the
  scheduler.

## Metrics

`pkg/executor` defines a small metrics sink:

```go
type MetricsSink interface {
    OnExecutorMetric(metric Metric)
}

type Metric struct {
    ExecutorName string
    TaskName     string
    Kind         string
    QueueDepth   int
    Workers      int
    Duration     time.Duration
    Err          error
}
```

Initial metric kinds:

- `task_submitted`
- `task_rejected`
- `task_started`
- `task_completed`
- `task_failed`
- `task_panicked`
- `executor_shutdown`

RuntimeGraph keeps its existing `RuntimeGraphMetricsSink`. Executor metrics are
separate because executor usage is general-purpose.

## Package Layout

```text
pkg/executor/
  doc.go
  errors.go
  executor.go
  future.go
  promise.go
  submit.go
  compose.go
  inline.go
  fixed_worker.go
  metrics.go
  example_test.go
```

RuntimeGraph integration remains in `pkg/disruptor`.

## Testing Requirements

Executor tests:

- typed submit completes future value.
- task errors complete future errors.
- panic is recovered and completes future error.
- promise completes exactly once under concurrent completion attempts.
- late observers are called exactly once.
- `Await` respects caller context cancellation.
- `AllOf`, `All`, `AnyOf`, and `Any` work without occupying worker slots.
- `ThenApply`, `ThenCompose`, and `Exceptionally` submit continuation work to
  the explicit executor.
- fixed worker rejects invalid worker and queue sizes.
- fixed worker reject policy returns `ErrSaturated`.
- fixed worker block policy respects submission context cancellation.
- shutdown stops accepting new tasks and waits for accepted tasks.
- race tests pass.

RuntimeGraph tests:

- workers greater than one can execute independent nodes concurrently.
- default workers equal one keeps the existing deterministic handler order.
- scheduler state remains single-owner.
- worker completion advances joins correctly.
- executor submission failure uses `RuntimeGraphExceptionKindExecutor`.
- caller-owned RuntimeGraph executors are not shut down by Disruptor.
- internally-created RuntimeGraph executors are shut down by Disruptor.
- handler error continue/retry/halt semantics match the inline scheduler.
- halt waits for in-flight workers before returning.
- metrics continue to emit node scheduled/completed and edge selected/skipped
  signals.

Failure matrix tests:

- executor closed during submit.
- executor saturated during submit.
- executor shutdown during submit.
- submission context canceled before enqueue.
- running task returns after promise cancellation attempt.
- continuation submission failure.
- `ThenCompose` outer failure.
- `ThenCompose` inner failure.
- `ThenCompose` inner cancellation.
- `All` with mixed success and failure.
- `Any` with mixed failure and success.
- panic handler notification without changing task recovery policy.

## Benchmark Requirements

Add benchmarks for:

- `BenchmarkFutureAwaitCompleted`
- `BenchmarkPromiseComplete`
- `BenchmarkExecutorSubmitInline`
- `BenchmarkExecutorSubmitFixedWorker`
- `BenchmarkFutureAllOf`
- `BenchmarkRuntimeGraphRoutingParallel`

RuntimeGraph benchmark expectations:

- default inline RuntimeGraph benchmarks should stay near the existing
  allocation profile.
- parallel RuntimeGraph benchmarks should report throughput and allocations
  separately because cross-goroutine scheduling has different costs.

## Quality Gates

Release-blocking verification:

```bash
make ci
go test -race -shuffle=on ./...
go test -run '^$' -bench='Benchmark(Future|Promise|Executor|RuntimeGraph)' \
  -benchmem -count=10 -cpu=1,2,4,8 ./... | tee bench.txt
benchstat benchmarks/baseline/baseline.txt bench.txt
```

`pkg/executor` and RuntimeGraph executor-path tests must include goroutine leak
checks using `go.uber.org/goleak`. Tests for parallel scheduling must use
deterministic coordination primitives such as barriers or manual executors, not
sleep-based timing guesses.

Regression policy:

- default inline RuntimeGraph allocation counts must not materially regress.
- executor benchmarks establish their own baseline because cross-goroutine
  scheduling has different costs from inline execution.
- benchmark conclusions must use `benchstat`; single-run results are smoke
  checks only.

CI must run ordinary tests, race tests, lint, vet, and a benchmark smoke target
that includes executor benchmarks.

## Compatibility

- Existing V1.3 APIs remain compatible.
- `WithRuntimeGraphWorkers` changes from a forward-compatible hook into an
  active configuration option.
- Default RuntimeGraph behavior remains deterministic and inline.
- Public executor APIs are new and may be used independently of Disruptor.

## Documentation Updates

Implementation must update:

- `README.md`
- `README.zh-CN.md`
- `docs/api-guide.md`
- `benchmarks/README.md`
- `examples/`

Examples must avoid naked anonymous function parameters in public APIs. They may
use named task structs or named `TaskFunc` variables.

Documentation acceptance criteria:

- `pkg/executor` has `doc.go` and executable examples.
- `docs/api-guide.md` documents executor ownership, Future/Promise semantics,
  backpressure, cancellation, and RuntimeGraph parallel execution.
- `README.md` and `README.zh-CN.md` include a concise executor example and a
  RuntimeGraph workers example.
- `benchmarks/README.md` explains executor benchmark interpretation.

## Design Checks

- No hidden unbounded goroutines are introduced by Future composition.
- No unbounded queues are introduced.
- RuntimeGraph workers do not mutate scheduler state.
- RuntimeGraph handler execution does not occupy the scheduler when an executor
  is configured.
- Cancellation is cooperative and documented.
- Shutdown is graceful and documented.
