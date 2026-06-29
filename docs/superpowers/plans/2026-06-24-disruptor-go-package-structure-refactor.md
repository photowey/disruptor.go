# Disruptor.go Package Structure Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Objective:** Split implementation-heavy code out of `pkg/disruptor` into
focused public packages while preserving clear ownership boundaries for the new
pre-1.0 API surface.

**Architecture:** Keep `pkg/disruptor` as the orchestration facade. Move event
contracts to `pkg/event`, sequence primitives to `pkg/sequence`, wait strategy
contracts to `pkg/wait`, metrics payloads to `pkg/metrics`, ring-buffer storage
to `pkg/ringbuffer`, processor lifecycle to `pkg/processor`, graph builders to
`pkg/graph` and `pkg/runtimegraph`, runtime variables to `pkg/runtimevars`, and
expressions to `pkg/expression`. Public constructors, interfaces, and options
live in the package that owns the concept.

**Tech Stack:** Go 1.26+, `go test`, `make lint`, existing benchmark suite,
public package boundaries plus internal sequencer/padding helpers.

**Current pass status:** The package split is implemented. `pkg/disruptor`
contains only facade orchestration and graph/runtime graph registration;
low-level ring buffer, processor, wait, metrics, sequence, graph, runtime graph,
expression, runtime variable, and event contracts are owned by focused packages.

---

### Task 1: Extract runtime variable package

**Files:**
- Create: `pkg/runtimevars/doc.go`
- Create: `pkg/runtimevars/path.go`
- Create: `pkg/runtimevars/bag.go`
- Create: `pkg/runtimevars/context.go`
- Create: `pkg/runtimevars/resolver.go`
- Modify: runtime graph and expression callers to import `pkg/runtimevars`
- Reuse existing callers without signature changes:
  `pkg/disruptor/runtime_graph_processors.go`,
  `pkg/processor/processor.go`

- [x] **Step 1: Write the failing tests**

Add or adjust tests so public runtime bag behavior, path validation, and event value resolution still pass through the facade after the helper move.

- [x] **Step 2: Move the implementation**

Move the runtime bag, runtime context, path validation, reflection resolver, and
merged lookup logic into `pkg/runtimevars`.

- [x] **Step 3: Run targeted tests**

Run: `go test ./pkg/runtimevars ./pkg/expression ./pkg/disruptor -run 'RuntimeBag|RuntimeExpression|RuntimeGraph' -count=1`

- [x] **Step 4: Run full verification**

Run: `go test ./... -count=1 && make lint`

### Task 2: Extract runtime expression package

**Files:**
- Create: `pkg/expression/doc.go`
- Create: `pkg/expression/compiler.go`
- Create: `pkg/expression/expression_test.go`
- Modify: runtime graph callers to import `pkg/expression`
- Reuse existing callers without signature changes:
  `pkg/runtimegraph/graph.go`

- [x] **Step 1: Write the failing tests**

Keep the existing runtime expression tests as the behavior contract, and add at least one test that exercises bool conversion and path lookup after the compiler delegates to the internal package.

- [x] **Step 2: Move the implementation**

Move scanner, parser, AST, and evaluator helpers into `pkg/expression`.

- [x] **Step 3: Run targeted tests**

Run: `go test ./pkg/expression -run 'RuntimeExpression' -count=1`

- [x] **Step 4: Run full verification**

Run: `go test ./... -count=1 && make lint`

### Task 3: Extract graph and runtime graph packages

**Files:**
- Review: `pkg/graph/graph.go`
- Review: `pkg/graph/snapshot.go`
- Review: `pkg/disruptor/graph_processors.go`
- Review: `pkg/runtimegraph/graph.go`
- Review: `pkg/disruptor/runtime_graph_processors.go`
- Create or update: `pkg/graph/*`, `pkg/runtimegraph/*`

- [x] **Step 1: Re-scan cycles and public coupling**

Confirm whether the graph helpers can move without importing `pkg/disruptor` from internal code.

- [x] **Step 2: Extract only if the boundary is clean**

Move node/edge normalization, snapshot building, and rendering helpers first.
Extraction requires a clean boundary that avoids cyclic adapters.

Result for this pass: extracted. Static graph builder and snapshots live in
`pkg/graph`; runtime graph builder and plans live in `pkg/runtimegraph`.
`pkg/disruptor` owns processor registration and scheduling.

- [x] **Step 3: Run targeted tests**

Run: `go test ./pkg/graph ./pkg/runtimegraph ./pkg/disruptor -run 'Graph|RuntimeGraph' -count=1`

- [x] **Step 4: Run full verification**

Run: `go test ./... -count=1 && make lint && go test ./benchmarks -run '^$' -bench='Benchmark(GraphTopology|RuntimeGraphRouting)' -benchmem -benchtime=100ms -count=1`
