# Disruptor.go Package Structure Design

## Status

Specification aligned with the implemented package structure.

Scope: package ownership, public import boundaries, examples, benchmarks, and
documentation for the split public API.

## Decision

Public APIs are split by responsibility. `pkg/disruptor` is the orchestration
facade for high-level consumer registration and lifecycle ownership. Lower-level
ring buffer, processor, wait strategy, metrics, sequence, graph, runtime graph,
expression, runtime variable, and event contracts live in their owner packages.

Callers import the package that owns the concept they use:

| Package | Responsibility |
| --- | --- |
| `pkg/disruptor` | Facade orchestration for fan-out, static graph, and runtime graph |
| `pkg/event` | Handler requests, node context, lifecycle hooks, exception handlers |
| `pkg/sequence` | Public sequence type and sequence readers |
| `pkg/wait` | Wait strategy interface and built-in blocking/busy-spin strategies |
| `pkg/metrics` | Backend-neutral metrics sink and metric payloads |
| `pkg/ringbuffer` | Preallocated event storage, barriers, producer options |
| `pkg/processor` | Event processor lifecycle and processor options |
| `pkg/graph` | Static dependency graph builder, validation, snapshots, Mermaid, DOT |
| `pkg/runtimegraph` | Conditional routing graph builder, edge conditions, routing snapshots |
| `pkg/expression` | Bool expression compiler used by runtime graph edges |
| `pkg/runtimevars` | Concurrent runtime variables and event value resolution |
| `internal/availability` | Contiguous publication scanning |
| `internal/padding` | Cache-line padding primitives and build-tag overrides |
| `internal/sequencer` | Sequence primitive and single/multi producer sequencers |

## Objectives

- Package boundaries are visible in code, examples, benchmarks, and docs.
- Public APIs remain interface-first and replaceable.
- Static graph APIs are exposed by `pkg/graph`; handler request and exception
  contracts are exposed by `pkg/event`.
- Ring buffer APIs and producer options are exposed by `pkg/ringbuffer`.
- Processor lifecycle APIs and processor options are exposed by `pkg/processor`.
- Package names are short, lowercase, and specific.
- Ring buffer and processor hot paths live in their owner packages.
- Low-level algorithms remain in `internal/` when they are not public imports.

## Out Of Scope

- Collapse public ownership back into one facade package.
- Add forwarding packages or re-export files for owner-package APIs.
- Introduce a dependency injection framework.
- Move every internal algorithm into a public package.
- Change Disruptor sequencing, wait strategy, or backpressure semantics.

## Public Package Boundaries

### `pkg/disruptor`

Owns high-level runtime orchestration:

- `Disruptor[T]`
- `HandleEventsWith`, `HandleGraph`, and `HandleRuntimeGraph`
- graph processor registration and runtime graph scheduler wiring
- facade lifecycle: `Start`, `Stop`, and `Wait`

It may depend on `pkg/event`, `pkg/graph`, `pkg/runtimegraph`,
`pkg/runtimevars`, `pkg/ringbuffer`, `pkg/processor`, `pkg/sequence`,
`pkg/metrics`, and external `pool.Executor` implementations. It must not
re-export those packages' primary types.

### `pkg/sequence`

Owns the stable public sequence surface:

- `Sequence`
- `Reader`
- `InitialValue`
- `New`
- `NewMinimumReader`

The concrete padded sequence primitive remains implemented in `internal`.

### `pkg/wait`

Owns producer and consumer wait strategy contracts:

- `Strategy`
- `Request`
- `CapacityRequest`
- `BlockingStrategy`
- `BusySpinStrategy`
- `NewBlockingStrategy`
- `NewBusySpinStrategy`

Wait strategies depend on `pkg/sequence`, not on `pkg/disruptor`.

### `pkg/metrics`

Owns optional instrumentation contracts:

- `Sink`
- `SinkFunc`
- `NoopSink`
- metric callback aliases
- `PublishMetric`
- `BatchMetric`
- `EventMetric`
- `WaitMetric`
- `ProcessorMetric`

Metric payloads carry primitive values and `event.Node` where graph context is
available. `PublishMetric.ProducerType` is a string label so metrics remain
independent from ring buffer producer enums.

### `pkg/ringbuffer`

Owns event storage and producer coordination:

- `RingBuffer[T]`
- `Barrier`
- `Option`
- `ProducerType`
- `WithProducerType`
- `WithWaitStrategy`
- `WithMetricsSink`
- ring-buffer scoped errors

The package depends on `internal/sequencer`, `pkg/event`, `pkg/sequence`,
`pkg/wait`, and `pkg/metrics`. It does not import `pkg/disruptor`.

### `pkg/processor`

Owns consumer processor lifecycle:

- `EventProcessor`
- `BatchEventProcessor[T]`
- `BatchConfig[T]`
- `HaltNotifier`
- `Option[T]`
- `WithExceptionHandler[T]`
- processor scoped errors

The package depends on `pkg/ringbuffer`, `pkg/event`, `pkg/sequence`,
`pkg/metrics`, and `pkg/runtimevars`. It does not import `pkg/disruptor`.

### `pkg/event`

Owns event processing contracts shared by fan-out, static graph, and runtime
graph scheduling:

- `Handler[T]`
- `HandlerFunc[T]`
- `Request[T]`
- `Node`
- `BatchStartHandler`
- `LifecycleHandler`
- `ExceptionHandler[T]`
- `ExceptionAction`
- built-in fatal, ignore, and retry exception handlers

### `pkg/graph`

Owns static topology definition and validation:

- `Graph[T]`
- `StartNode` and `EndNode`
- `NodeOption[T]`
- `Join`
- `Snapshot`
- deterministic `Mermaid` and `DOT` export
- `ErrInvalid`, `ErrFrozen`, and `ErrHandled`

Static graph edges are unconditional. Terminal edges are explicit and maintained
by developers.

### `pkg/runtimegraph`

Owns conditional topology definition:

- `RuntimeGraph[T]`
- `EdgeCondition[T]`
- `EdgeConditionRequest[T]`
- `WhenCondition`
- `WhenExpression`
- runtime graph node and edge options
- `RuntimeGraphSnapshot`
- `ErrInvalid`, `ErrFrozen`, `ErrHandled`, and `ErrNoRoute`

The package owns graph construction and edge evaluation contracts. The scheduler
that consumes a built plan still lives in `pkg/disruptor`.

### `pkg/expression`

Owns the built-in bool expression engine:

- `Expression`
- `Compiler`
- `BoolExpression`
- `Request`
- `Value`, `ValueKind`, and `ValueConverter`
- `NewCompiler`
- `WithValueConverter`
- `ErrInvalid`

The expression engine has no dependency on `pkg/disruptor`.

### `pkg/runtimevars`

Owns runtime variable lookup:

- `Bag`
- `Context`
- `ContextView`
- `Variables`
- `Provider[T]`
- `Resolver[T]`
- path validation
- merged lookup order used by runtime graph expressions

Variables are concurrency-safe and use last-write-wins semantics.

## Import-Cycle Rule

Public leaf packages must not import `pkg/disruptor`.

Allowed dependency direction:

```text
pkg/disruptor
  -> pkg/event
  -> pkg/graph
  -> pkg/runtimegraph
  -> pkg/runtimevars
  -> pkg/ringbuffer
  -> pkg/processor
  -> pkg/sequence
  -> pkg/metrics
  -> github.com/photowey/pool.go/pkg/pool

pkg/processor
  -> pkg/ringbuffer
  -> pkg/event
  -> pkg/sequence
  -> pkg/metrics
  -> pkg/runtimevars

pkg/ringbuffer
  -> pkg/event
  -> pkg/sequence
  -> pkg/wait
  -> pkg/metrics

pkg/runtimegraph
  -> pkg/event
  -> pkg/expression
  -> pkg/graph
  -> pkg/runtimevars

pkg/expression
  -> pkg/runtimevars
```

`pkg/event`, `pkg/sequence`, `pkg/wait`, `pkg/metrics`, `pkg/ringbuffer`,
`pkg/processor`, `pkg/graph`, `pkg/expression`, and `pkg/runtimevars` are usable
without importing `pkg/disruptor`.

## Examples And Benchmarks

Examples and benchmarks demonstrate the package split directly:

- handler request types use `event.Request[T]`
- handler slices use `[]event.Handler[T]`
- ring buffers and producer options use `pkg/ringbuffer`
- processor options use `pkg/processor`
- metrics sinks and payloads use `pkg/metrics`
- wait strategies use `pkg/wait`
- static graphs use `topology "github.com/photowey/disruptor.go/pkg/graph"`
- runtime graphs use `runtimegraph`
- retry/fatal/ignore exception handlers use `pkg/event`
- examples import owner packages for ring buffer, processor, metrics, wait,
  graph, and event contracts

## Documentation Updates

Required docs:

- `README.md`
- `README.zh-CN.md`
- `docs/api-guide.md`
- V1.2 runtime graph design
- benchmark notes if benchmark scenarios or imports change

Architecture diagrams show the public package split.

## Acceptance Criteria

- `pkg/disruptor` contains high-level fan-out, static graph, and runtime graph
  orchestration files.
- Ring buffer, barrier, processor, wait, metrics, sequence, event, graph,
  runtime graph, expression, and runtime variable APIs are owned by their
  dedicated packages.
- Examples compile against the split packages.
- Benchmarks compile against the split packages.
- Current docs and diagrams use the split package names.
- `go test ./... -count=1` passes.
- `go test ./... -race -count=1` passes or any failure is explained.
- `make lint` passes.
- Runtime graph and static graph benchmark smoke tests pass.
