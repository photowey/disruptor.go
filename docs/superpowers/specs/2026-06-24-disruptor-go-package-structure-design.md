# Disruptor.go Package Structure Design

## Status

Specification aligned with the implemented package structure.

Scope: pre-v1.2.0 hardening with an intentional breaking API cleanup. The
package split prioritizes ownership clarity over compatibility aliases.

## Decision

Public APIs are split by responsibility. `pkg/disruptor` is no longer the only
public facade and must not re-export graph, runtime graph, expression, runtime
variable, or event contracts.

Callers import the package that owns the concept they use:

| Package | Responsibility |
| --- | --- |
| `pkg/disruptor` | Ring buffer, disruptor facade, barriers, processors, wait strategies, metrics |
| `pkg/event` | Handler requests, node context, lifecycle hooks, exception handlers |
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
- Compatibility aliases such as `disruptor.MustGraph` or
  `disruptor.EventRequest`.
- Package names are short, lowercase, and specific.
- Processor and ring-buffer hot paths remain in `pkg/disruptor`.
- Low-level algorithms remain in `internal/` when they are not public imports.

## Out Of Scope

- Preserve the previous single-package facade.
- Add glue files that only forward old names to new packages.
- Introduce a dependency injection framework.
- Move every internal algorithm into a public package.
- Change Disruptor sequencing, wait strategy, or backpressure semantics.

## Public Package Boundaries

### `pkg/disruptor`

Owns runtime orchestration:

- `RingBuffer[T]`
- `Disruptor[T]`
- `BatchEventProcessor[T]`
- `WaitStrategy`
- producer type options
- metrics payloads and sinks
- `HandleEventsWith`, `HandleGraph`, and `HandleRuntimeGraph`

It may depend on `pkg/event`, `pkg/graph`, `pkg/runtimegraph`, and
`pkg/runtimevars`, but it must not re-export their primary types.

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

pkg/runtimegraph
  -> pkg/event
  -> pkg/expression
  -> pkg/graph
  -> pkg/runtimevars

pkg/expression
  -> pkg/runtimevars
```

`pkg/event`, `pkg/graph`, `pkg/expression`, and `pkg/runtimevars` are usable
without importing `pkg/disruptor`.

## Examples And Benchmarks

Examples and benchmarks demonstrate the package split directly:

- handler request types use `event.Request[T]`
- handler slices use `[]event.Handler[T]`
- static graphs use `topology "github.com/photowey/disruptor.go/pkg/graph"`
- runtime graphs use `runtimegraph`
- retry/fatal/ignore exception handlers use `pkg/event`
- examples contain no old `disruptor.*` graph or event aliases

## Documentation Updates

Required docs:

- `README.md`
- `README.zh-CN.md`
- `docs/api-guide.md`
- V1.2 runtime graph design
- benchmark notes if benchmark scenarios or imports change

Architecture diagrams show the public package split.

## Acceptance Criteria

- `pkg/disruptor` no longer contains event, graph, runtime graph, expression, or
  runtime variable builder files.
- Compatibility re-export files for old graph/event APIs are absent.
- Examples compile against the split packages.
- Benchmarks compile against the split packages.
- Current docs and diagrams use the split package names.
- `go test ./... -count=1` passes.
- `go test ./... -race -count=1` passes or any failure is explained.
- `make lint` passes.
- Runtime graph and static graph benchmark smoke tests pass.
