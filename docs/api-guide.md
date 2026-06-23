# API Guide

This guide documents the public V1 surface of `github.com/photowey/disruptor.go/pkg/disruptor`.

## Event Storage

`RingBuffer[T]` preallocates value slots in `[]T` and returns pointers to those
slots:

```go
type LongEvent struct {
    Value int64
}

factory := disruptor.EventFactoryFunc[LongEvent](func() LongEvent {
    return LongEvent{}
})
rb, err := disruptor.NewRingBuffer(factory, 1024)
```

`rb.Get(sequence)` returns `*LongEvent`, so producers and consumers mutate or
read the ring slot directly instead of copying generic values.

## Low-Level Ring Buffer

Use `RingBuffer[T]` when you want direct control over claim, translate, and
publish steps.

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

`PublishEvent` is the safe convenience path. After a successful claim, it always
publishes the claimed sequence, even if the translator panics.

## High-Level Disruptor

Use `Disruptor[T]` for the common V1 topology: one ring buffer with parallel
consumers. Every handler receives every event.

```go
d, err := disruptor.New(factory, 1024)
if err != nil {
    return err
}

_, err = d.HandleEventsWith(handlerA, handlerB)
if err != nil {
    return err
}
if err := d.Start(ctx); err != nil {
    return err
}
defer d.Stop()
```

`Wait` waits for all processors. If any processor fails, the facade stops peer
processors so `Wait` can return the collected terminal error instead of hanging.

## Options

Ring buffer options:

- `WithProducerType(ProducerTypeSingle)` or `WithProducerType(ProducerTypeMulti)`
- `WithWaitStrategy(strategy)`
- `WithMetricsSink(sink)`

Processor options:

- `WithExceptionHandler[T](handler)`

Options are separated by lifecycle stage so a processor option cannot be passed
to ring-buffer construction.

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

The request payloads leave room for future fields without growing long parameter
lists.

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

Use `MetricsSinkFunc` when you only need a subset of callbacks. Use
`NoopMetricsSink` when a non-nil sink is useful in tests or adapters.

## Testing And Benchmarking

Recommended local checks:

```bash
go test ./...
go test -race ./...
go test -bench=. -benchmem -count=10 ./...
```

Use the package-level micro benchmarks for hot-path operations and the
`benchmarks` package for end-to-end and channel comparison groups.
