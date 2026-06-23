# disruptor.go

High-performance Disruptor pattern implementation for Go, with generic ring
buffers, cancellable sequencing, recovery hooks, metrics, examples, and
benchmarks.

The public API favors interfaces and replaceable components. Core algorithms can
evolve internally without forcing users to rewrite producers, consumers, metrics
adapters, or recovery policies.

## Install

```bash
go get github.com/photowey/disruptor.go
```

Import the public package:

```go
import "github.com/photowey/disruptor.go/pkg/disruptor"
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"

    "github.com/photowey/disruptor.go/pkg/disruptor"
)

type LongEvent struct {
    Value int64
}

func main() {
    ctx := context.Background()

    d, err := disruptor.New(
        disruptor.EventFactoryFunc[LongEvent](func() LongEvent { return LongEvent{} }),
        1024,
    )
    if err != nil {
        panic(err)
    }

    done := make(chan int64, 1)
    _, err = d.HandleEventsWith(disruptor.EventHandlerFunc[LongEvent](func(
        request disruptor.EventRequest[LongEvent],
    ) error {
        done <- request.Event.Value
        return nil
    }))
    if err != nil {
        panic(err)
    }
    if err := d.Start(ctx); err != nil {
        panic(err)
    }

    err = d.RingBuffer().PublishEvent(ctx, disruptor.EventTranslatorFunc[LongEvent](func(
        request disruptor.TranslateRequest[LongEvent],
    ) {
        request.Event.Value = 42
    }))
    if err != nil {
        panic(err)
    }

    fmt.Println(<-done)

    d.Stop()
    if err := d.Wait(); err != nil {
        panic(err)
    }
}
```

## API Shape

- `RingBuffer[T]` is the low-level API for claiming, mutating, and publishing
  preallocated event slots.
- `Disruptor[T]` is the high-level facade for one ring buffer with parallel
  consumers. Each V1 consumer receives all events.
- `EventFactory[T]`, `EventTranslator[T]`, `EventHandler[T]`,
  `ExceptionHandler[T]`, `WaitStrategy`, and `MetricsSink` are interfaces.
- `XxxFunc` adapters are available where callbacks are useful without exposing
  anonymous function types in public signatures.
- `context.Context` is accepted by blocking producer and processor operations so
  waits can be cancelled.

## Layout

The public package is `pkg/disruptor`. Internal algorithm boundaries live under
`internal/`:

```text
internal/
  availability/   contiguous publication scanning
  padding/        cache-line padding primitives
  sequencer/      sequence primitive plus single/multi producer sequencers

pkg/disruptor/    public API, ring buffer facade, barriers, processors, metrics
benchmarks/       end-to-end and channel comparison benchmarks
examples/         runnable usage examples
docs/             API and design documentation
```

`pkg/disruptor.Sequence` is re-exported from `internal/sequencer`, so external
users get a stable public type while internal sequencing algorithms remain
replaceable.

## Recovery

The default exception handler is fail-fast. You can replace it with ignore or
bounded retry behavior:

```go
retryHandler, err := disruptor.NewRetryExceptionHandler[LongEvent](
    2,
    disruptor.ExceptionActionHalt,
)
if err != nil {
    panic(err)
}

_, err = d.HandleEventsWithOptions(
    []disruptor.EventHandler[LongEvent]{handler},
    disruptor.WithExceptionHandler[LongEvent](retryHandler),
)
```

Handler panics are recovered and routed through the same exception handler path.
Producer translator panics still publish the claimed sequence first, then
re-panic to the caller so consumers do not get stuck behind an unpublished slot.

## Metrics

Metrics are opt-in and backend-neutral. The default sink is nil, so hot paths
short-circuit before measuring or dispatching.

```go
metrics := disruptor.MetricsSinkFunc{
    Publish: func(metric disruptor.PublishMetric) {
        // record publish batch size, sequence, duration, error, etc.
    },
    EventHandled: func(metric disruptor.EventMetric) {
        // record handler duration and errors.
    },
}
```

## Examples

Runnable examples live under `examples/`:

- `examples/basic`
- `examples/multi_consumer`
- `examples/metrics`
- `examples/error_recovery`

Run one with:

```bash
go run ./examples/basic
```

## Benchmarks

Benchmarks are part of release readiness:

```bash
go test ./...
go test -race ./...
go test -bench=. -benchmem -count=10 ./...
```

See `benchmarks/README.md` for end-to-end and channel comparison groups.

Channels remain the right default for ordinary ownership transfer and simple
synchronization. Use this library when benchmarks show that you need high
throughput, low allocation, broadcast-to-many consumers, or controlled
backpressure.
