# API Guide

This guide documents the public V1 surface of
`github.com/photowey/disruptor.go/pkg/disruptor`.

## Event Storage

`RingBuffer[T]` preallocates value slots in `[]T` and returns pointers to those
slots. Producers and consumers mutate or read the ring slot directly instead of
copying generic values.

```go
type LongEvent struct {
    Value int64
}

type LongEventFactory struct{}

func (LongEventFactory) NewEvent() LongEvent {
    return LongEvent{}
}

rb, err := disruptor.NewRingBuffer(LongEventFactory{}, 1024)
if err != nil {
    return err
}

event := rb.Get(0)
event.Value = 42
```

For quick adapters, the package also exposes `EventFactoryFunc[T]`,
`EventTranslatorFunc[T]`, and `EventHandlerFunc[T]`. Public examples use named
types so production code can keep dependencies explicit and replaceable.

## Low-Level Ring Buffer

Use `RingBuffer[T]` when you want direct control over claim, mutate, and publish
steps.

```go
sequence, err := rb.Next(ctx)
if err != nil {
    return err
}

event := rb.Get(sequence)
event.Value = 42
rb.Publish(sequence)
```

Batched claims return the high sequence:

```go
hi, err := rb.NextN(ctx, 16)
if err != nil {
    return err
}

lo := hi - 16 + 1
for sequence := lo; sequence <= hi; sequence++ {
    rb.Get(sequence).Value = sequence
}
rb.PublishRange(lo, hi)
```

Non-blocking claims return `ErrInsufficientCapacity` when gating sequences would
be overrun:

```go
sequence, err := rb.TryNext()
if errors.Is(err, disruptor.ErrInsufficientCapacity) {
    return nil
}
if err != nil {
    return err
}

rb.Get(sequence).Value = 42
rb.Publish(sequence)
```

`TryNextN(n)` is the batched non-blocking form.

`PublishEvent` is the safe convenience path. After a successful claim, it always
publishes the claimed sequence, even if the translator panics.

```go
type LongEventTranslator struct {
    Value int64
}

func (t LongEventTranslator) Translate(
    request disruptor.TranslateRequest[LongEvent],
) {
    request.Event.Value = t.Value
}

err := rb.PublishEvent(ctx, LongEventTranslator{Value: 42})
```

Backpressure is controlled by gating sequences:

```go
consumerSequence := disruptor.NewSequence(disruptor.InitialSequenceValue)
rb.AddGatingSequences(consumerSequence)
defer rb.RemoveGatingSequence(consumerSequence)
```

The high-level `Disruptor[T]` and `BatchEventProcessor[T]` manage their own
gating sequences.

## High-Level Disruptor

Use `Disruptor[T]` for the common V1 topology: one ring buffer with parallel
consumers. Every handler receives every event.

```go
type LongEventHandler struct {
    Done chan<- int64
}

func (h LongEventHandler) OnEvent(
    request disruptor.EventRequest[LongEvent],
) error {
    h.Done <- request.Event.Value
    return nil
}

d, err := disruptor.New(LongEventFactory{}, 1024)
if err != nil {
    return err
}

_, err = d.HandleEventsWith(LongEventHandler{Done: done})
if err != nil {
    return err
}

if err := d.Start(ctx); err != nil {
    return err
}
defer d.Stop()
```

`HandleEventsWithOptions` attaches processor-specific options, currently
`WithExceptionHandler[T](handler)`.

```go
retryHandler, err := disruptor.NewRetryExceptionHandler[LongEvent](
    2,
    disruptor.ExceptionActionHalt,
)
if err != nil {
    return err
}

_, err = d.HandleEventsWithOptions(
    []disruptor.EventHandler[LongEvent]{LongEventHandler{Done: done}},
    disruptor.WithExceptionHandler[LongEvent](retryHandler),
)
```

`Wait` waits for all processors. If any processor fails, the facade stops peer
processors so `Wait` can return the collected terminal error instead of hanging.

```go
d.Stop()
if err := d.Wait(); err != nil {
    return err
}
```

## Event Processors

`NewBatchEventProcessor` is the lower-level processor API. It is useful when you
need to wire barriers and dependencies yourself.

```go
barrier := rb.NewBarrier()
processor, err := disruptor.NewBatchEventProcessor(
    rb,
    barrier,
    LongEventHandler{Done: done},
)
if err != nil {
    return err
}
```

The processor adds its sequence as a ring-buffer gating sequence and removes it
when the processor exits.

## Options

Ring buffer options:

- `WithProducerType(ProducerTypeSingle)` or `WithProducerType(ProducerTypeMulti)`
- `WithWaitStrategy(strategy)`
- `WithMetricsSink(sink)`

Processor options:

- `WithExceptionHandler[T](handler)`

Options are separated by lifecycle stage so a processor option cannot be passed
to ring-buffer construction.

`ProducerTypeMulti` is the default. It tracks claimed and published sequences
with per-slot availability metadata, so consumers do not observe a later
published sequence while an earlier claimed sequence is still unpublished.

`ProducerTypeSingle` is the lighter path for one producer goroutine. It assumes
the single producer publishes claimed sequences in order, including batch ranges.
Use `ProducerTypeMulti` when multiple producers publish concurrently or when
publication can happen out of claim order.

`ProducerTypeUnknown` and out-of-range producer values are rejected. A nil wait
strategy is rejected. A nil metrics sink disables metrics.

## Wait Strategies

Built-ins:

- `NewBlockingWaitStrategy()`
- `NewBusySpinWaitStrategy()`

Custom wait strategies implement:

```go
type WaitStrategy interface {
    WaitFor(request disruptor.WaitRequest) (int64, error)
    WaitForCapacity(request disruptor.CapacityWaitRequest) error
    SignalAll()
}
```

`WaitRequest` carries the request context, requested sequence, cursor sequence,
dependent sequence, and barrier. `CapacityWaitRequest` is the public alias for
the sequencer capacity-wait payload. The payload style keeps the interface
stable when future fields are added.

## Event Handlers

Required:

```go
type EventHandler[T any] interface {
    OnEvent(request disruptor.EventRequest[T]) error
}
```

Optional:

```go
type BatchStartHandler interface {
    OnBatchStart(request disruptor.BatchStartRequest) error
}

type LifecycleHandler interface {
    OnStart(ctx context.Context) error
    OnShutdown(ctx context.Context) error
}
```

The processor detects optional capabilities through type assertions. Panics from
`OnEvent`, `OnBatchStart`, `OnStart`, and `OnShutdown` are recovered into errors
and routed through the configured exception policy.

## Exception Handling

Default behavior is fail-fast:

```go
handler := disruptor.NewFatalExceptionHandler[LongEvent]()
```

Built-ins:

- `NewFatalExceptionHandler[T]()` returns `ExceptionActionHalt`.
- `NewIgnoreExceptionHandler[T]()` returns `ExceptionActionContinue`.
- `NewRetryExceptionHandler[T](maxRetries, exhaustedAction)` retries a failed
  event up to `maxRetries` times before returning the exhausted action.

Actions:

- `ExceptionActionHalt`: stop the processor and return the error from `Wait`.
- `ExceptionActionContinue`: advance the failed sequence and continue.
- `ExceptionActionRetry`: retry the same sequence without advancing.

`NewRetryExceptionHandler` rejects negative retry counts. Its exhausted action
must be either `ExceptionActionHalt` or `ExceptionActionContinue`.

## Metrics

`MetricsSink` is backend-neutral:

```go
type MetricsSink interface {
    OnPublish(request disruptor.PublishMetric)
    OnBatchStart(request disruptor.BatchMetric)
    OnEventHandled(request disruptor.EventMetric)
    OnWait(request disruptor.WaitMetric)
    OnProcessorState(request disruptor.ProcessorMetric)
}
```

Use a named sink when wiring production telemetry:

```go
type CountingMetricsSink struct{}

func (CountingMetricsSink) OnPublish(metric disruptor.PublishMetric) {}
func (CountingMetricsSink) OnBatchStart(metric disruptor.BatchMetric) {}
func (CountingMetricsSink) OnEventHandled(metric disruptor.EventMetric) {}
func (CountingMetricsSink) OnWait(metric disruptor.WaitMetric) {}
func (CountingMetricsSink) OnProcessorState(metric disruptor.ProcessorMetric) {}
```

`MetricsSinkFunc` and typed callback aliases (`PublishMetricFunc`,
`BatchMetricFunc`, `EventMetricFunc`, `WaitMetricFunc`, and
`ProcessorMetricFunc`) are available for lightweight adapters. Use
`NoopMetricsSink` when a non-nil sink is useful in tests or integration
adapters.

## Testing And Benchmarking

Recommended local checks:

```bash
go test ./...
go test -race ./...
go test -run '^$' -bench=. -benchmem -benchtime=100ms -count=10 -cpu=1,2,4,8 ./...
benchstat benchmarks/baseline/baseline.txt /tmp/disruptor-new.txt
```

Use the package-level microbenchmarks for hot-path operations and the
`benchmarks` package for end-to-end, M/N producer-consumer, channel comparison,
baseline, and tail-latency groups.
