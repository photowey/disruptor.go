# Disruptor.go Package Structure Design

## Status

Approved direction, written for implementation planning.

Target: pre-v1.2.0 hardening with no public API break.

## Background

`pkg/disruptor` has grown from the original public package into a mixed package
containing public contracts, facade construction, ring-buffer operations,
processor lifecycle, static graph topology, runtime graph routing, expression
evaluation, runtime variables, metrics, and tests.

This is still manageable at the API level because users import one package, but
the implementation boundaries are becoming harder to reason about. The package
now contains more than thirty Go files and several large domains that evolve at
different speeds.

## Decision

Keep `pkg/disruptor` as the only public package for v1.x.

Move implementation-heavy code behind internal packages. The public package
remains the facade and owns the stable user-facing names:

- constructors
- option functions
- interfaces
- request and metric payloads
- errors
- public graph and runtime graph builders

Internal packages own algorithms and state machines. They must not be imported
by users and must not leak new public import paths.

## Goals

- Preserve the current import path:
  `github.com/photowey/disruptor.go/pkg/disruptor`.
- Keep public API names stable through the v1.x line.
- Make each implementation area easier to review, test, benchmark, and evolve.
- Prepare runtime graph and future topic-routing work without making
  `pkg/disruptor` a catch-all package.
- Keep package names short, lowercase, and specific.
- Avoid premature public subpackages.

## Non-Goals

- Do not create public packages such as `pkg/disruptor/graph` in v1.x.
- Do not change examples, benchmarks, or external import paths as part of the
  package split.
- Do not redesign the Disruptor runtime semantics.
- Do not introduce a dependency injection framework.
- Do not move every file in one large mechanical commit.

## Current Responsibility Map

The current root package contains these responsibility groups:

| Area | Current files |
| --- | --- |
| Public facade | `disruptor.go`, `doc.go`, `options.go`, `errors.go` |
| Ring buffer | `ring_buffer.go`, `sequence.go`, `sequence_reader.go` |
| Processor runtime | `event_processor.go`, `barrier.go`, `wait_strategy.go` |
| User contracts | `event_handler.go`, `translator.go`, `metrics.go`, `node_context.go` |
| Static graph | `graph.go`, `graph_join.go`, `graph_snapshot.go`, `graph_processors.go` |
| Runtime graph | `runtime_graph.go`, `runtime_graph_processors.go` |
| Runtime expressions | `runtime_expression.go` |
| Runtime variables | `runtime_variables.go` |

Existing root-level `internal/` packages already hold low-level private
algorithms:

- `internal/availability`
- `internal/padding`
- `internal/sequencer`

## Target Layout

The target keeps one public package and moves engines to module-private
packages:

```text
pkg/disruptor/
  doc.go
  errors.go
  options.go
  disruptor.go
  ring_buffer.go
  event_handler.go
  translator.go
  metrics.go
  node_context.go
  wait_strategy.go
  graph.go
  runtime_graph.go

internal/
  availability/
  padding/
  sequencer/
  ring/
  processor/
  graph/
  runtimegraph/
  expression/
  runtimevars/
```

`pkg/disruptor` remains the package users import. The internal packages are
module-private implementation packages. They can be used by `pkg/disruptor`,
examples, and benchmarks inside this module, but external users cannot import
them.

## Package Boundaries

### Public Facade

`pkg/disruptor` should contain:

- public interfaces such as `EventHandler[T]`, `WaitStrategy`, `MetricsSink`,
  `RuntimeVariables`, and `EdgeCondition[T]`
- public request payloads such as `EventRequest[T]`, `WaitRequest`,
  `GraphSnapshot`, `RuntimeGraphSnapshot`, and `RuntimeGraphMetric`
- public constructors such as `New`, `NewRingBuffer`, `NewGraph`,
  `NewRuntimeGraph`, and `NewRuntimeExpressionCompiler`
- public option functions such as `WithWaitStrategy`,
  `WithGraphExceptionHandler`, and `WithRuntimeGraphNoRouteAction`
- thin methods that delegate to internal engines

The facade may contain small validation helpers when they directly protect the
public API, but algorithm-heavy code should move out.

### Internal Ring

`internal/ring` should own ring-buffer state and slot access once extracted.
The public `RingBuffer[T]` can remain a facade struct that delegates to a ring
engine.

This package can continue to depend on:

- `internal/sequencer`
- `internal/availability`
- `internal/padding`

### Internal Processor

`internal/processor` should own processor loops, barrier coordination, lifecycle
state, batch notification, and gating behavior.

This extraction is more sensitive because processor code currently uses public
payloads and handlers. The package must avoid importing `pkg/disruptor` to
prevent import cycles. Implementation options:

- Keep processor code in `pkg/disruptor` until public payload boundaries are
  stable enough to extract.
- Or introduce internal request and handler contracts, then let the facade adapt
  public handlers to those internal contracts.

The first implementation pass should prefer the lower-risk path: extract
expression, runtime variables, and graph algorithms before extracting the
processor loop.

### Internal Graph

`internal/graph` should own static graph algorithms:

- node and edge normalization
- terminal edge validation
- source, leaf, entry, and exit computation
- cycle checks
- deterministic snapshot ordering
- Mermaid and DOT rendering helpers

It should not know about `EventHandler[T]`. The facade remains responsible for
binding handler values to graph nodes.

### Internal Runtime Graph

`internal/runtimegraph` should own runtime routing plans and route-state
execution:

- compiled plan shape
- start and end terminal handling
- selected and skipped edge accounting
- active join semantics
- no-route state transitions
- scheduler state

The facade remains responsible for public builder methods, option processing,
exception handler contracts, and metrics payload names.

### Internal Expression

`internal/expression` should own the runtime expression engine:

- scanner
- parser
- AST nodes
- compiled expression
- bool conversion
- numeric and bitwise evaluation
- converter chain

`pkg/disruptor.NewRuntimeExpressionCompiler` remains public and wraps this
internal compiler.

### Internal Runtime Variables

`internal/runtimevars` should own:

- concurrent runtime bag implementation
- lookup helpers
- default struct, JSON-tag, and string-map event resolvers
- merged variable lookup order

`pkg/disruptor` keeps the public `RuntimeVariables`, `RuntimeBag`,
`RuntimeContext`, `RuntimeVariablesProvider[T]`, and `EventValueResolver[T]`
contracts.

## Import-Cycle Rule

Internal packages must not import `pkg/disruptor`.

The root public package may import internal packages. Internal packages can
share small contracts with each other, but those contracts must not require the
public package.

This rule is the main reason the migration must be incremental rather than a
single file move.

## Migration Phases

### Phase 0: Public Surface Snapshot

Capture the current public API before moving code:

- list exported names
- run `go test ./...`
- run `make lint`
- run graph and runtime graph benchmark smoke tests
- verify examples still compile

This creates a baseline for detecting accidental API drift.

### Phase 1: Low-Cycle Extractions

Move implementation code that has the fewest ties to public handlers:

1. `runtime_expression.go` internals to `internal/expression`
2. `runtime_variables.go` internals to `internal/runtimevars`
3. graph validation and rendering helpers to `internal/graph`

Keep public types and constructors in `pkg/disruptor`.

### Phase 2: Runtime Graph Engine

Move route-plan and run-state implementation to `internal/runtimegraph`.

Keep public `RuntimeGraph[T]`, `RuntimeGraphProcessors`,
`RuntimeGraphMetric`, and option functions in `pkg/disruptor`.

The facade should adapt public handlers, conditions, variables, exception
handlers, and metrics into internal engine contracts.

### Phase 3: Processor and Ring Review

Only after Phase 1 and Phase 2 are stable, review whether moving
`event_processor.go`, `barrier.go`, and `ring_buffer.go` brings enough value.

If extracted, the public package should still expose the same concrete public
types. Any internal engine type should remain unexported or exported only inside
an internal package.

## Testing Strategy

Tests should follow the package boundary:

- Public behavior tests remain in `pkg/disruptor`.
- Internal algorithm tests move beside the internal package they validate.
- Examples remain black-box through `pkg/disruptor`.
- Benchmarks keep using public APIs unless the benchmark is specifically for an
  internal algorithm.

Required regression checks for every phase:

```text
go test ./... -count=1
make lint
go test ./benchmarks -run '^$' -bench='Benchmark(GraphTopology|RuntimeGraphRouting)' -benchmem -benchtime=100ms -count=1
```

For processor or ring changes, also run:

```text
go test ./... -race -count=1
make ci
```

## Documentation Updates

After implementation, update:

- `README.md` architecture diagram if internal boundaries are shown
- `README.zh-CN.md` architecture diagram
- `docs/api-guide.md` only if public APIs change, which is not expected
- benchmark notes only if benchmark names or scenarios change, which is not
  expected

Examples should not need code changes because the public import path remains the
same.

## Acceptance Criteria

- External users still import only `pkg/disruptor`.
- No public subpackages are introduced.
- No public API is removed or renamed.
- `pkg/disruptor` has thinner facade files and fewer algorithm-heavy files.
- Internal packages have clear, testable responsibilities.
- No import cycles are introduced.
- Full tests, lint, race tests, and benchmark smoke checks pass.

## Open Implementation Notes

- Generic type aliases should be avoided unless they are proven necessary and
  compatible with the module's supported Go version.
- Package names should stay specific and lowercase: `runtimegraph`,
  `runtimevars`, and `expression` are acceptable; `utils` and `helper` are not.
- The first implementation PR should be small enough to review by domain.
- A purely mechanical move is not enough. The goal is clearer ownership, not
  only a different directory tree.
