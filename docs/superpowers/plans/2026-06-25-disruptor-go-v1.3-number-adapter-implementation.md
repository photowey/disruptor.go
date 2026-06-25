# Disruptor.go V1.3 NumberAdapter Implementation Plan

## Goal

Implement the V1.3.0 NumberAdapter design so expression runtime values can be
extended with decimal-like, money-like, and big-number-like custom values
without adding a decimal dependency to the core module.

## Impact Notes

GitNexus impact analysis before implementation:

- `Value`: HIGH risk, because it is the expression engine's normalized value
  structure and is used by parser, evaluator, converters, comparisons, and
  bitwise operations.
- `NewCompiler`: LOW risk, direct effect on runtime graph compiler wiring.
- `evaluateRuntimeComparison`: LOW risk, direct effect on runtime comparison
  evaluation.
- `expressionValueToBool`: LOW risk, direct effect on final expression bool
  conversion.

The implementation keeps the high-risk `Value` change additive and preserves
the built-in scalar fast path.

## Implementation Steps

1. Add TDD coverage for the public NumberAdapter contract.
2. Add public expression APIs:
   - `NumberKind`
   - `Number`
   - `ValueNumber`
   - `NumericComparator`
   - `NumberCompareRequest`
   - `NumberBoolConverter`
   - `NumberBoolRequest`
   - `NumberAdapter`
   - `OrderedNumberAdapter`
   - `DefaultNumberAdapterOrder`
   - `WithNumberAdapter`
3. Extend compiler wiring:
   - keep default converters first
   - sort number adapters by `Order()` ascending
   - preserve registration order for equal orders
   - register adapters in converter, comparator, and final bool converter
     chains
4. Extend evaluation:
   - allow ordinary variables to convert to `ValueNumber`
   - allow typed object variables to reach adapter conversion
   - keep typed scalar values allocation-conscious
   - try adapter comparators only after built-in comparison cannot handle the
     pair
   - apply number-to-bool conversion only to the final result
5. Add RuntimeGraph regression coverage for adapter condition errors.
6. Add a fake decimal adapter benchmark path.
7. Update `README.md`, `README.zh-CN.md`, `docs/api-guide.md`,
   `examples/runtime_graph`, and benchmark docs.
8. Verify:
   - `go test ./... -count=1`
   - `go test ./... -race -count=1`
   - `golangci-lint run --timeout=10m`
   - targeted runtime graph/expression benchmarks
   - `gitnexus detect_changes`

## Acceptance Checks

- Core module does not import decimal dependencies.
- Existing V1.2 scalar expression behavior remains unchanged.
- Built-in `int`, `uint`, and `float` comparisons do not route through adapter
  chains.
- Ordinary and typed variables can become `ValueNumber`.
- Number adapter ordering is deterministic.
- Adapter errors in RuntimeGraph conditions are recoverable through the
  existing runtime graph exception path.

## Verification Results

Completed locally on 2026-06-25:

- `go test ./... -count=1`: 207 tests passed in 22 packages.
- `go test ./... -race -count=1`: 207 tests passed in 22 packages.
- `golangci-lint run --timeout=10m`: no issues found.
- `BenchmarkRuntimeGraphRouting/single_path`: `0 B/op`, `0 allocs/op`.
- `BenchmarkRuntimeGraphRouting/expression_branch`: `0 B/op`, `0 allocs/op`.
- `BenchmarkRuntimeGraphRouting/active_join`: `0 B/op`, `0 allocs/op`.
- `BenchmarkExpressionNumberAdapterComparison`: `0 B/op`, `0 allocs/op`.
- `BenchmarkViewLookupValueJSONTagResolver`: `0 B/op`, `0 allocs/op`.
- `git diff --check`: clean.
- `gitnexus detect_changes`: reports `critical` risk because the implementation
  intentionally touches expression core symbols (`Value`, `NewCompiler`,
  runtime comparison, and final bool conversion).
