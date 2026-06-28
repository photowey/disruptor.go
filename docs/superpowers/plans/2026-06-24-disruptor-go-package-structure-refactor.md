# Disruptor.go Package Structure Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Objective:** Split implementation-heavy code out of `pkg/disruptor` into
focused internal packages while preserving the existing public API and import
path.

**Architecture:** Keep `pkg/disruptor` as a stable public facade. Move low-cycle helpers into `internal/runtimevars` first, then extract the expression engine into `internal/expression`, then review whether static graph helpers can move into `internal/graph` without creating import cycles. Public constructors, interfaces, and option functions stay in `pkg/disruptor`; internal packages own algorithms and state.

**Tech Stack:** Go 1.26+, `go test`, `make lint`, existing benchmark suite, internal package boundaries.

**Current pass status:** Task 1 and Task 2 are complete. Task 3 was reviewed;
graph extraction is intentionally deferred because the current graph code still
shares public snapshot types, handler binding, processor registration, and
runtime graph helpers. Extracting it safely requires a separate adapter design.

---

### Task 1: Extract runtime variable internals

**Files:**
- Create: `internal/runtimevars/doc.go`
- Create: `internal/runtimevars/path.go`
- Create: `internal/runtimevars/bag.go`
- Create: `internal/runtimevars/context.go`
- Create: `internal/runtimevars/resolver.go`
- Modify: `pkg/disruptor/runtime_variables.go`
- Modify: `pkg/disruptor/runtime_expression.go`
- Reuse existing callers without signature changes:
  `pkg/disruptor/runtime_graph_processors.go`,
  `pkg/disruptor/event_processor.go`

- [x] **Step 1: Write the failing tests**

Add or adjust tests so public runtime bag behavior, path validation, and event value resolution still pass through the facade after the helper move.

- [x] **Step 2: Move the implementation**

Move the runtime bag, runtime context, path validation, reflection resolver, and merged lookup logic into `internal/runtimevars`, keeping only public adapters in `pkg/disruptor`.

- [x] **Step 3: Run targeted tests**

Run: `go test ./pkg/disruptor -run 'RuntimeBag|RuntimeExpression|RuntimeGraph' -count=1`

- [x] **Step 4: Run full verification**

Run: `go test ./... -count=1 && make lint`

### Task 2: Extract runtime expression internals

**Files:**
- Create: `internal/expression/doc.go`
- Create: `internal/expression/runtime_expression.go`
- Create: `internal/expression/expression_test.go`
- Modify: `pkg/disruptor/runtime_expression.go`
- Modify: `pkg/disruptor/errors.go`
- Reuse existing callers without signature changes:
  `pkg/disruptor/runtime_graph.go`

- [x] **Step 1: Write the failing tests**

Keep the existing runtime expression tests as the behavior contract, and add at least one test that exercises bool conversion and path lookup after the compiler delegates to the internal package.

- [x] **Step 2: Move the implementation**

Move scanner, parser, AST, and evaluator helpers into `internal/expression`, leaving the public compiler and exported types in `pkg/disruptor`.

- [x] **Step 3: Run targeted tests**

Run: `go test ./pkg/disruptor -run 'RuntimeExpression' -count=1`

- [x] **Step 4: Run full verification**

Run: `go test ./... -count=1 && make lint`

### Task 3: Review graph boundary extraction

**Files:**
- Review: `pkg/disruptor/graph.go`
- Review: `pkg/disruptor/graph_snapshot.go`
- Review: `pkg/disruptor/graph_processors.go`
- Review: `pkg/disruptor/runtime_graph.go`
- Review: `pkg/disruptor/runtime_graph_processors.go`
- Create only if safe: `internal/graph/*`

- [x] **Step 1: Re-scan cycles and public coupling**

Confirm whether the graph helpers can move without importing `pkg/disruptor` from internal code.

- [x] **Step 2: Extract only if the boundary is clean**

Move node/edge normalization, snapshot building, and rendering helpers first.
Extraction requires a clean boundary that avoids cyclic adapters.

Result for this pass: not extracted. The current graph boundary is not clean
enough for a safe mechanical move because static graph, runtime graph, and
processor registration all share public snapshot and handler-bearing types.

- [x] **Step 3: Run targeted tests**

Run: `go test ./pkg/disruptor -run 'Graph|RuntimeGraph' -count=1`

- [x] **Step 4: Run full verification**

Run: `go test ./... -count=1 && make lint && go test ./benchmarks -run '^$' -bench='Benchmark(GraphTopology|RuntimeGraphRouting)' -benchmem -benchtime=100ms -count=1`
