# Go Disruptor Design

Date: 2026-06-23

## Goal

Build a Go implementation of the LMAX Disruptor pattern for high-performance in-process event exchange.

The design favors Go idioms over a direct Java class mirror. Public APIs are interface-oriented and replaceable where users need extension, while core sequencing algorithms stay internal so they can be rewritten, optimized, or accelerated without breaking users.

## Non-Goals For V1

- No consumer dependency graph DSL such as `After` or `Then`.
- No pull-style `Poller`.
- No public custom sequencer injection.
- No SIMD or AVX implementation in V1, only an internal extension point.
- No direct dependency on Prometheus, OpenTelemetry, or any metrics backend.
- No complex batch rewind implementation.

## Project Layout

The module is a library. Public package code lives under `pkg/disruptor`; internal algorithm and optimization details live under `internal`.

```text
disruptor.go/
  go.mod

  pkg/
    disruptor/
      barrier.go
      benchmark_test.go
      disruptor.go
      errors.go
      event_handler.go
      event_processor.go
      metrics.go
      options.go
      ring_buffer.go
      sequence.go
      translator.go
      wait_strategy.go

  internal/
    availability/
      scanner.go
      scalar.go
    padding/
      cacheline.go
      cacheline_default_*.go
      cacheline_override_*.go
    sequencer/
      sequence.go
      sequencer.go
      sequencer_bench_test.go
      single_producer.go
      multi_producer.go

  benchmarks/
    barrier_bench_test.go
    e2e_bench_test.go
    latency_bench_test.go
    sequencer_bench_test.go
    README.md
    baseline/

  examples/
    basic/
    error_recovery/
    metrics/
    multi_consumer/

  docs/
    api-guide.md
```

Users import:

```go
import "github.com/photowey/disruptor.go/pkg/disruptor"
```

## Design Principles

- Public API is interface-oriented where substitution is useful.
- Constructors return concrete types, not interfaces.
- Public function parameters do not expose bare anonymous function types.
- Callback-style APIs use named interfaces plus `XxxFunc` adapters.
- `context.Context` is passed explicitly and is never stored in structs.
- Blocking producer and consumer paths support cancellation.
- Event mutation happens through slot pointers, so value events are not accidentally copied.
- Hot-path defaults are allocation-conscious and metrics use a nil fast path unless configured.
- Internal packages may define additional interfaces for implementation flexibility, but those interfaces are not compatibility promises.

## Party Mode Review Decisions

The BMAD-style roundtable accepted the overall direction and locked the following adjustments before implementation:

- V1 uses value slots in `[]T` and exposes `*T` through `Get`, `TranslateRequest`, `EventRequest`, and `EventException`.
- `internal/sequencer` never imports `pkg/disruptor`; public `Sequence` is re-exported from the internal primitive and barrier construction stays in the public package.
- Options are split into `RingBufferOption` and `ProcessorOption[T]` so generic handlers cannot be attached to the wrong component.
- Metrics are backend-neutral, opt-in, low-cardinality, and must use a nil fast path on hot code paths.
- Benchmarking is a V1 release gate with channel comparison groups, baseline comparison, allocation checks, and tail-latency reporting.
- SIMD or AVX scanning, timeout handlers, custom sequencer injection, and dependency graph DSL remain V2 extension points.

## Reference Implementation Learnings

The local `github.com/smarty/go-disruptor` implementation has a different API philosophy, but several design choices are worth learning from.

Adopt these ideas:

- Keep the producer path conceptually simple: claim one or more slots, mutate the ring slots, then publish the claimed sequence range.
- Treat batched claims as first-class. `NextN(ctx, n)` plus `PublishRange(lo, hi)` should be benchmarked and documented as a core performance path.
- Keep single-producer and multi-producer sequencers as separate implementations. The single-producer path can cache the slowest consumer sequence and avoid unnecessary atomics; the multi-producer path needs explicit per-slot availability tracking.
- Use a composite barrier abstraction for “minimum sequence across many consumers”. Even if V1 only exposes parallel consumers, the internal shape should leave room for handler groups and dependency graphs.
- Benchmark sequence operations, single barrier reads, composite barrier reads, claim/publish, channel comparisons, single-producer flows, multi-producer flows, and batched publish flows separately.
- Explain clearly that channels are still the default Go tool for ordinary ownership transfer; this library exists for measured high-throughput, low-allocation, fan-out and controlled-backpressure cases.

Do not inherit these parts:

- Do not make users manage the ring buffer directly in normal usage. V1 owns preallocated `[]T` slots and exposes `*T` safely through `RingBuffer[T]`.
- Do not use magic negative sequence values as errors. Public APIs return `error`.
- Do not use an irrevocable multi-producer atomic add that can spin forever after shutdown. Blocking producer APIs must be cancellable through `context.Context`.
- Do not let handler panics kill processor goroutines without recovery. Handler failures go through `ExceptionHandler[T]`.
- Do not rely on `go:linkname` to reach runtime internals for acquire/release atomics in V1. Race-detector compatibility and Go-version stability matter more than a narrow optimization.

## V1 Scope

V1 implements a complete push-based production and consumption flow:

- Generic `RingBuffer[T]`.
- Preallocated events via `EventFactory[T]`.
- Internal single-producer and multi-producer sequencers.
- Blocking and busy-spin wait strategies.
- Batch event processors managed by the library.
- Multiple parallel consumers, each receiving all events.
- Gating sequences so producers do not overwrite unconsumed events.
- Low-level `RingBuffer` API and high-level `Disruptor` facade.
- Metrics hooks.
- Benchmarks with Go channel comparison groups.
- README, examples, and API guide sufficient for a user to run a basic case quickly and understand when to use `Disruptor` versus `RingBuffer`.
- A reproducible benchmark and quality gate for release readiness.

## Event Storage

`RingBuffer[T]` stores `[]T` and exposes mutable slots as `*T`. This avoids the common Go generic trap where returning `T` copies the value and mutations do not update the ring slot.

The recommended event model is a value slot:

```go
type LongEvent struct {
    Value int64
}

factory := disruptor.EventFactoryFunc[LongEvent](func() LongEvent {
    return LongEvent{}
})
```

`RingBuffer[LongEvent].Get(sequence)` returns `*LongEvent`, so producers and consumers mutate or read the preallocated slot directly.

Pointer event types are allowed for adapter use cases, but then `Get` returns `*T`, which is a pointer to the stored pointer. V1 examples and benchmarks should prefer value slot events unless there is a measured reason not to.

## Named Function Adapters

Public APIs use named interfaces and optional `XxxFunc` adapters.

```go
type EventFactory[T any] interface {
    NewEvent() T
}

type EventFactoryFunc[T any] func() T

func (fn EventFactoryFunc[T]) NewEvent() T {
    return fn()
}
```

The same pattern applies to translators, exception handlers, and metrics adapters where useful.

## Ring Buffer API

The blocking producer API accepts `context.Context` so capacity waits can be cancelled.

```go
type RingBuffer[T any] struct {
    // unexported fields
}

func NewRingBuffer[T any](
    factory EventFactory[T],
    size int,
    opts ...RingBufferOption,
) (*RingBuffer[T], error)

func (r *RingBuffer[T]) Next(ctx context.Context) (int64, error)
func (r *RingBuffer[T]) NextN(ctx context.Context, n int64) (int64, error)
func (r *RingBuffer[T]) TryNext() (int64, error)
func (r *RingBuffer[T]) TryNextN(n int64) (int64, error)

func (r *RingBuffer[T]) Get(sequence int64) *T
func (r *RingBuffer[T]) Publish(sequence int64)
func (r *RingBuffer[T]) PublishRange(lo, hi int64)

func (r *RingBuffer[T]) PublishEvent(
    ctx context.Context,
    translator EventTranslator[T],
) error

func (r *RingBuffer[T]) AddGatingSequences(sequences ...*Sequence)
func (r *RingBuffer[T]) RemoveGatingSequence(sequence *Sequence) bool
func (r *RingBuffer[T]) NewBarrier(dependencies ...*Sequence) Barrier
```

`NextN(ctx, n)` and `TryNextN(n)` return the high sequence. Callers compute the low sequence as `lo := hi - n + 1` before calling `PublishRange(lo, hi)`.

`Publish` does not return an error. Once a sequence is claimed, it must be published to avoid sequence holes.

## Event Translator

Translators use a request payload and do not return errors.

```go
type EventTranslator[T any] interface {
    Translate(request TranslateRequest[T])
}

type EventTranslatorFunc[T any] func(request TranslateRequest[T])

func (fn EventTranslatorFunc[T]) Translate(request TranslateRequest[T]) {
    fn(request)
}

type TranslateRequest[T any] struct {
    Context  context.Context
    Event    *T
    Sequence int64
}
```

Work that can fail should happen before claiming a sequence.

```go
value, err := parse(input)
if err != nil {
    return err
}
err = rb.PublishEvent(ctx, disruptor.EventTranslatorFunc[LongEvent](func(request disruptor.TranslateRequest[LongEvent]) {
    request.Event.Value = value
}))
```

`PublishEvent` must guarantee publication after a successful claim. Its implementation should use `defer r.Publish(sequence)` immediately after `Next(ctx)` succeeds. If a translator panics, the sequence is still published and the panic is re-raised; leaving a claimed but unpublished sequence would break consumer progress.

## Producer Type And Options

Options are used for configuration that can grow over time.

```go
type ProducerType uint8

const (
    ProducerTypeUnknown ProducerType = iota
    ProducerTypeSingle
    ProducerTypeMulti
)

type RingBufferOption interface {
    applyRingBuffer(*ringBufferConfig) error
}

type ProcessorOption[T any] interface {
    applyProcessor(*processorConfig[T]) error
}

func WithProducerType(producerType ProducerType) RingBufferOption
func WithWaitStrategy(strategy WaitStrategy) RingBufferOption
func WithMetricsSink(sink MetricsSink) RingBufferOption
func WithExceptionHandler[T any](handler ExceptionHandler[T]) ProcessorOption[T]
```

Options are created through `With*` constructors. Ring buffer and processor configuration are kept separate so a generic option can never be applied to the wrong lifecycle stage. The unexported `apply*` methods keep internal configuration details private and let the package validate option compatibility inside `NewRingBuffer`, `New`, or `HandleEventsWithOptions`.

V1 does not expose `WithSequencer`.

## Sequence

`Sequence` is public because low-level users need it for gating and custom processors.

```go
const InitialSequenceValue int64 = -1

type Sequence struct {
    // padded atomic.Int64
}

func NewSequence(initial int64) *Sequence
func (s *Sequence) Value() int64
func (s *Sequence) Store(value int64)
func (s *Sequence) Add(delta int64) int64
func (s *Sequence) CompareAndSwap(oldValue, newValue int64) bool
```

`Store` is preferred over `Set` because it matches `sync/atomic` semantics.

`pkg/disruptor/sequence.go` re-exports the internal primitive so public users can use `Sequence` without creating an import cycle:

```go
type Sequence = sequencer.Sequence

const InitialSequenceValue = sequencer.InitialSequenceValue

func NewSequence(initial int64) *Sequence
```

## Internal Sequencer Boundary

Sequencer implementations are internal in V1.

```text
internal/sequencer/
  sequencer.go
  single_producer.go
  multi_producer.go
```

The internal interface can evolve without breaking users.

```go
type Sequencer interface {
    Next(ctx context.Context) (int64, error)
    NextN(ctx context.Context, n int64) (int64, error)
    TryNext() (int64, error)
    TryNextN(n int64) (int64, error)
    Publish(sequence int64)
    PublishRange(lo, hi int64)
    Cursor() *Sequence
    AddGatingSequences(sequences ...*Sequence)
    RemoveGatingSequence(sequence *Sequence) bool
    HighestPublished(lowerBound, availableSequence int64) int64
    Available(sequence int64) bool
}
```

The public API lets users select `ProducerTypeSingle` or `ProducerTypeMulti`, but not inject arbitrary sequencing algorithms.

`internal/sequencer` must not import `pkg/disruptor`; the public package owns barrier construction and re-exports the sequence primitive instead of the reverse.

`internal/padding` owns cache-line padding constants. It must not hard-code a single universal cache-line size. The default is selected at compile time by `GOARCH`, following Go's own approximation: 32 bytes for arm/mips families, 64 bytes for most common targets, 128 bytes for arm64/ppc64, and 256 bytes for s390x. Explicit build tags `disruptor_cacheline_32`, `disruptor_cacheline_64`, `disruptor_cacheline_128`, and `disruptor_cacheline_256` are supported for benchmarking and unusual deployment targets.

Sequencer implementation rules:

- Single-producer sequencer may use plain producer-owned fields for claimed sequence state, with atomic publication for consumer visibility.
- Single-producer capacity checks should cache the slowest gating sequence and refresh it only on possible wrap contention.
- Multi-producer sequencer must represent claimed versus published slots separately, so consumers never observe a claimed-but-unpublished sequence.
- Multi-producer availability should use per-slot round or lap metadata indexed by `sequence & mask` and scanned contiguously from the requested lower bound.
- Blocking `Next(ctx)` and `NextN(ctx, n)` must remain interruptible while waiting for capacity.
- `TryNext()` and `TryNextN(n)` perform bounded non-blocking attempts and return `ErrInsufficientCapacity` rather than spinning.

## Availability Scanner

Multi-producer publication needs to distinguish claimed sequences from published sequences. V1 uses a scalar scanner.

The scanner is internal:

```go
type Scanner interface {
    HighestPublished(request ScanRequest) int64
}

type ScanRequest struct {
    LowerBound        int64
    AvailableSequence int64
    Availability      Checker
}

type Checker interface {
    Available(sequence int64) bool
}
```

V1 uses `scalar.Scanner`. Future SIMD or AVX scanners can be added behind this boundary by build tags and CPU feature detection without changing public APIs.

## Barrier

`Barrier` is public because custom processors and low-level users need it. The implementation remains unexported.

```go
type Barrier interface {
    WaitFor(ctx context.Context, sequence int64) (int64, error)
    Cursor() int64
    Alert()
    ClearAlert()
    CheckAlert() error
    Alerted() bool
}
```

`RingBuffer.NewBarrier(dependencies ...*Sequence)` is the public construction entry.

## Wait Strategy

`WaitStrategy` uses a request payload to avoid long parameter lists and support future extension.

```go
type SequenceReader interface {
    Value() int64
}

type WaitStrategy interface {
    WaitFor(request WaitRequest) (int64, error)
    WaitForCapacity(request CapacityWaitRequest) error
    SignalAll()
}

type WaitRequest struct {
    Context           context.Context
    RequestedSequence int64
    CursorSequence    SequenceReader
    DependentSequence SequenceReader
    Barrier           Barrier
}

type CapacityWaitRequest struct {
    Context            context.Context
    RequestedSequences int64
    CurrentSequence    int64
    WrapPoint          int64
    GatingSequence     SequenceReader
}
```

V1 built-ins:

- `NewBlockingWaitStrategy()`
- `NewBusySpinWaitStrategy()`

`BlockingWaitStrategy` listens to `Context.Done()` for both consumer waits and producer capacity waits. `BusySpinWaitStrategy` periodically checks `Context.Err()` and, for consumer waits, `Barrier.CheckAlert()`.
Producer capacity waits use `WaitForCapacity` so backpressure can be tuned without hiding an uninterruptible spin loop inside the sequencer.

## Event Handler

Event handling uses one main interface plus optional small interfaces.

```go
type EventHandler[T any] interface {
    OnEvent(request EventRequest[T]) error
}

type EventRequest[T any] struct {
    Context    context.Context
    Event      *T
    Sequence   int64
    EndOfBatch bool
}
```

Optional capabilities:

```go
type BatchStartHandler interface {
    OnBatchStart(request BatchStartRequest) error
}

type BatchStartRequest struct {
    Context    context.Context
    BatchSize  int64
    QueueDepth int64
}

type LifecycleHandler interface {
    OnStart(ctx context.Context) error
    OnShutdown(ctx context.Context) error
}
```

Processors use safe type assertions to detect optional capabilities.
`TimeoutHandler` is deferred to V2 together with timeout-oriented wait strategies.

## Exception Handling And Recovery

Handler failures are delegated to an exception handler that returns a recovery action.

```go
type ExceptionAction uint8

const (
    ExceptionActionUnknown ExceptionAction = iota
    ExceptionActionHalt
    ExceptionActionContinue
    ExceptionActionRetry
)

type ExceptionHandler[T any] interface {
    HandleEventException(request EventException[T]) ExceptionAction
    HandleStartException(request LifecycleException) ExceptionAction
    HandleShutdownException(request LifecycleException) ExceptionAction
}

type EventException[T any] struct {
    Context  context.Context
    Event    *T
    Sequence int64
    Err      error
}

type LifecycleException struct {
    Context context.Context
    Err     error
}
```

Default behavior is fail-fast:

- `Halt`: store the failed sequence, stop the processor, and return the error from `Wait`.
- `Continue`: store the failed sequence and continue with the next event.
- `Retry`: do not store the failed sequence and process it again.

Processors recover panics from `OnEvent`, `OnStart`, and `OnShutdown`, wrap the recovered value in an error, and route it through the same exception handler path. Producer-side translator panics are different: the claimed sequence is published, then the panic is re-raised to the caller.

V1 includes:

- `NewFatalExceptionHandler[T]()` returning `Halt`.
- `NewIgnoreExceptionHandler[T]()` returning `Continue`.
- A simple retry handler with a bounded max attempt count.

Complex batch rewind remains a V2 item.

## Event Processor Lifecycle

Processors are library-managed goroutines with explicit lifecycle methods.

```go
type EventProcessor interface {
    Start(ctx context.Context) error
    Stop()
    Wait() error
    Sequence() *Sequence
}
```

Rules:

- `Start(ctx)` starts one goroutine and returns an error if already running.
- `Stop()` is safe to call multiple times.
- `Wait()` blocks until the processor exits and returns its terminal error.
- `Stop()` cancels the processor context, alerts dependent barriers, and calls `WaitStrategy.SignalAll()` so blocked producers and consumers wake promptly.
- The processor sequence always advances for successfully handled events.
- On default fatal handler errors, the failed sequence is stored before shutdown so producers are not permanently gated by an already-read slot.

## High-Level Disruptor Facade

The facade handles the V1 simple topology: one ring buffer with parallel consumers. Each consumer receives all events.

```go
type Disruptor[T any] struct {
    // unexported fields
}

func New[T any](
    factory EventFactory[T],
    size int,
    opts ...RingBufferOption,
) (*Disruptor[T], error)

func (d *Disruptor[T]) RingBuffer() *RingBuffer[T]
func (d *Disruptor[T]) HandleEventsWith(handlers ...EventHandler[T]) ([]EventProcessor, error)
func (d *Disruptor[T]) HandleEventsWithOptions(
    handlers []EventHandler[T],
    opts ...ProcessorOption[T],
) ([]EventProcessor, error)
func (d *Disruptor[T]) Start(ctx context.Context) error
func (d *Disruptor[T]) Stop()
func (d *Disruptor[T]) Wait() error
```

V1 supports parallel consumers only. Consumer dependency graph support is deferred.

## Metrics Hooks

Metrics are part of V1. The core package defines a backend-neutral hook interface. The default behavior is no metrics: the configured sink is nil and hot paths short-circuit before measuring or dispatching.

```go
type MetricsSink interface {
    OnPublish(request PublishMetric)
    OnBatchStart(request BatchMetric)
    OnEventHandled(request EventMetric)
    OnWait(request WaitMetric)
    OnProcessorState(request ProcessorMetric)
}

type PublishMetricFunc func(PublishMetric)
type BatchMetricFunc func(BatchMetric)
type EventMetricFunc func(EventMetric)
type WaitMetricFunc func(WaitMetric)
type ProcessorMetricFunc func(ProcessorMetric)

type MetricsSinkFunc struct {
    Publish        PublishMetricFunc
    BatchStart     BatchMetricFunc
    EventHandled   EventMetricFunc
    Wait           WaitMetricFunc
    ProcessorState ProcessorMetricFunc
}

type NoopMetricsSink struct{}
```

Payloads are named types:

```go
type PublishMetric struct {
    ProducerType ProducerType
    Sequence     int64
    BatchSize    int64
    Duration     time.Duration
    Err          error
}

type BatchMetric struct {
    BatchSize  int64
    QueueDepth int64
}

type EventMetric struct {
    Sequence int64
    Duration time.Duration
    Err      error
}

type WaitMetric struct {
    RequestedSequence int64
    AvailableSequence int64
    Duration          time.Duration
    Strategy          string
    Err               error
}

type ProcessorMetric struct {
    State string
    Err   error
}
```

Core does not import Prometheus or OpenTelemetry. Adapters can be added later in separate packages, such as:

```text
pkg/observability/prometheus
pkg/observability/otel
```

Metric labels in adapters must remain low-cardinality.
The zero value of `NoopMetricsSink` does nothing, and `MetricsSinkFunc` lets users wire one or two callbacks without implementing the full interface. The adapter implements `MetricsSink` by checking whether each field is nil before invoking it. Hot-path metric emission must short-circuit before calling `time.Now`, building payloads, or dispatching to an interface when no sink is configured.

## Errors

Expected control-flow errors are sentinels:

```go
var (
    ErrAlerted              = errors.New("disruptor: alerted")
    ErrClosed               = errors.New("disruptor: closed")
    ErrInsufficientCapacity = errors.New("disruptor: insufficient capacity")
    ErrInvalidBufferSize    = errors.New("disruptor: invalid buffer size")
    ErrInvalidSequence      = errors.New("disruptor: invalid sequence")
)
```

Rules:

- Error strings are lowercase and have no trailing punctuation.
- Returned errors are wrapped with context using `%w`.
- Errors are either returned or logged, not both.
- Core library code does not print or log by default.

## Benchmark Plan

Benchmarks are a V1 requirement because this library is high-performance infrastructure.

### Micro Benchmarks

- `Sequence.Value`
- `Sequence.Store`
- `Sequence.Add`
- `Sequence.CompareAndSwap`
- `RingBuffer.Get`
- `RingBuffer.Next/Publish`
- `RingBuffer.NextN/PublishRange`
- `RingBuffer.TryNext`
- availability scanner scalar path
- single-sequence barrier read
- composite barrier read

### Sequencer Benchmarks

- Single producer claim and publish.
- Multi producer claim and publish.
- Capacity-full backpressure behavior.
- Highest published sequence scanning.
- Batch claim and publish for `1`, `4`, `16`, `64`, and `256` slots.

### End-To-End Benchmarks

- Disruptor 1 producer / 1 consumer with blocking wait strategy.
- Disruptor 1 producer / 1 consumer with busy-spin wait strategy.
- Disruptor multi producer / 1 consumer with blocking wait strategy.
- Disruptor multi producer / 1 consumer with busy-spin wait strategy.
- Disruptor 1 producer / multiple parallel consumers.
- Disruptor multi producer / multiple parallel consumers.
- Batched publish paths where handlers process contiguous sequence ranges.

### Go Channel Comparison Groups

Channel comparison groups are required so users can see when Disruptor is worth the complexity.

- Unbuffered `chan T`.
- Buffered `chan T` with capacity equal to ring buffer size.
- Buffered `chan *T` pointer events.
- Non-blocking channel send/receive loops using `select { default: }`, documented as a spin comparison rather than idiomatic application code.
- `sync.Cond` plus a simple slice or ring queue.

The benchmark docs must explain that channels remain the right tool for general ownership transfer and simple synchronization. Disruptor targets high throughput, low allocation, broadcast-to-many consumers, and controlled backpressure.

### Benchmark Matrix

Each benchmark family should be runnable across a stable matrix so results stay comparable over time.

- Ring size: `1024`, `65536`, `1048576`.
- Payload shape: small value slots, padded value slots, and pointer-adapter events.
- Producer/consumer topology: `1/1`, `1/N`, `M/1`, and `M/N`.
- Claim/publish batch size: `1`, `4`, `16`, `64`, `256`.
- Wait strategy: blocking and busy-spin.
- `GOMAXPROCS`: `1`, `2`, `4`, `8`.

### Release Gate

Benchmarking is part of V1 release readiness, not a later nice-to-have.

- `go test ./...`
- `go test -race ./...`
- `go test -bench=. -benchmem -count=10 -cpu=1,2,4,8 ./...`
- `benchstat` comparison against a checked-in baseline.
- Hot-path `allocs/op == 0` unless a benchmark explicitly documents why an allocation is required.
- Investigate any sustained `ns/op` regression above 5 percent.
- Block release on regression above 10 percent or any unexpected allocation growth.
- Keep separate queue versus broadcast comparison groups so channel results are not conflated.
- Add a tail-latency harness for publish-to-handle p50, p95, and p99 under blocking and busy-spin modes.

### Benchmark Commands

Because the module targets Go 1.26, new benchmarks should use `b.Loop()`.

```bash
go test -bench=. -benchmem -count=10 ./...
go test -bench=BenchmarkE2E -benchmem -count=10 ./benchmarks | tee /tmp/disruptor-bench.txt
benchstat /tmp/disruptor-old.txt /tmp/disruptor-new.txt
```

Benchmarks should report:

- `ns/op`
- `B/op`
- `allocs/op`
- custom `events/s`

Important runs should include:

```bash
go test -bench=. -benchmem -count=10 -cpu=1,2,4,8 ./...
```

Performance claims require statistically meaningful `benchstat` output.

## Testing Plan

Tests are executable specifications.

- Table-driven unit tests for public API validation.
- Race tests using `go test -race ./...`.
- Cancellation tests for producer `Next(ctx)` and wait strategies.
- Sequence wrap tests for power-of-two ring buffer behavior.
- Multi-producer availability tests to ensure claimed-but-unpublished slots are not consumed.
- Publish-after-claim tests, including translator panic paths that must still publish the claimed sequence.
- Batched `NextN` and `PublishRange` tests for low/high sequence math.
- Exception handler tests for `Halt`, `Continue`, and bounded `Retry`.
- Metrics sink tests proving nil default fast path plus explicit noop and custom sink delivery.
- Lifecycle idempotency tests for `Start`, `Stop`, `Wait`, barrier alerting, and wait-strategy wakeups.
- Blocked producer shutdown tests proving `Next(ctx)` and `NextN(ctx, n)` return instead of spinning forever.
- Leak checks for processor shutdown paths, using `go.uber.org/goleak` or an equivalent pattern.
- Memory-ordering and false-sharing regression coverage around sequence publication and cacheline padding.

## V2 Backlog

Deferred items are explicitly tracked so the V1 API can leave room for them.

- Consumer dependency graph DSL: `After`, `Then`, and dependency-aware barriers.
- Pull-style `Poller[T]`.
- SIMD or AVX availability scanner backend.
- Prometheus and OpenTelemetry adapter packages.
- Full batch rewind strategy.
- Additional wait strategies: timeout, yielding, phased backoff, sleeping.
- Public custom sequencer extension, if a strong external need appears.
- `TimeoutHandler` and `ErrTimeout` support paired with timeout-aware wait strategies.
- Rich examples for pipeline and diamond topologies after DSL support exists.

## Open Decisions

There are no blocking open decisions for V1 design. Remaining choices are implementation-level details to settle during the implementation plan, such as exact file split, benchmark names, and internal struct layout.
