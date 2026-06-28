# Disruptor.go V1.3 Number Adapter Design

## Status

Implemented.

Release tag: `v1.3.0`

This design extends the V1.2 runtime expression engine with a high-level number
extension API. The feature is intended for decimal, money, big number, and other
domain-specific numeric values without adding a decimal dependency to the core
module.

## Objectives

- Add an expression number extension point that is easy for application
  developers to implement.
- Default `int`, `uint`, and `float` expression behavior stays compatible with
  V1.2.
- The default runtime graph hot path and default expression benchmarks stay
  stable.
- Support custom number comparison, final-result bool conversion, and conversion
  from both ordinary variables and typed variables.
- Allow multiple number adapters with deterministic ordering.
- Public API remains interface-first and replaceable through compiler options.
- Decimal, money, and big number dependencies stay outside the core module.

## Out Of Scope

- Core `pkg/expression` has no `github.com/shopspring/decimal` dependency.
- Core expression syntax has no decimal literals in V1.3.0.
- Core string semantics are unchanged. Strings are parsed as numbers only when
  a registered number adapter explicitly handles that comparison.
- `&&`, `||`, and `!` have no implicit number-to-bool conversion on
  intermediate operands.
- RuntimeGraph scheduling, START/END semantics, NoRoute behavior, and graph
  validation rules are not changed by this release.

## Design Summary

V1.3.0 introduces `ValueNumber` as an expression value kind and `NumberAdapter`
as the high-level extension API.

The public adapter is intentionally coarse-grained:

- convert raw or typed values into `ValueNumber`.
- compare custom numbers.
- convert custom numbers to bool when the entire expression result is a number.

Internally, `NumberAdapter` is a composition of smaller interfaces so each
behavior remains testable:

```go
type NumberAdapter interface {
    ValueConverter
    NumericComparator
    NumberBoolConverter
}
```

Default built-in numeric handling remains first. Custom adapters handle only
cases that the built-in path does not handle.

## Public API Sketch

### Number Values

```go
type NumberKind string

type Number interface {
    NumberKind() NumberKind
}
```

`NumberKind` is intentionally open-ended. Adapter packages define their own
number kind constants, for example `NumberKindDecimal` or `NumberKindMoney`.
The core module only defines the type.

`ValueKind` gains `ValueNumber`:

```go
type ValueKind uint8

const (
    ValueInvalid ValueKind = iota
    ValueBool
    ValueInt
    ValueUint
    ValueFloat
    ValueString
    ValueObject
    ValueNumber
    ValueNil
)

type Value struct {
    Kind   ValueKind
    Value  any
    Number Number
    Bool   bool
    Int    int64
    Uint   uint64
    Float  float64
    String string
}
```

`ValueObject` remains a generic unsupported object. `ValueNumber` means the
expression engine recognizes the value as an extension number, while the adapter
owns its concrete semantics.

### Adapter Interfaces

```go
type NumericComparator interface {
    CompareNumber(request NumberCompareRequest) (result int, handled bool, err error)
}

type NumberCompareRequest struct {
    Left  Value
    Right Value
}

type NumberBoolConverter interface {
    ConvertNumberToBool(request NumberBoolRequest) (value bool, handled bool, err error)
}

type NumberBoolRequest struct {
    Value Value
}
```

`CompareNumber` returns a three-way comparison:

- `result < 0`: left is less than right.
- `result == 0`: left equals right.
- `result > 0`: left is greater than right.
- `handled == false`: the adapter does not handle this pair.

If `handled == true` and `err != nil`, evaluation stops and returns the error.
Later adapters are not tried.

### Ordered Adapters

Multiple adapters are supported. Ordering follows an optional interface:

```go
const DefaultNumberAdapterOrder = 0

type OrderedNumberAdapter interface {
    NumberAdapter
    Order() int
}
```

Ordering rules:

- Built-in expression numeric handling is always first and is not part of
  adapter ordering.
- Registered adapters are sorted by `Order()` ascending.
- Adapters without `Order()` use `DefaultNumberAdapterOrder`.
- Adapters with the same order keep registration order.
- The first adapter returning `handled == true` wins.

This matches the familiar "lower order runs earlier" model while preserving Go's
optional interface style.

### Compiler Option

```go
func WithNumberAdapter(adapter NumberAdapter) CompilerOption
```

Example:

```go
compiler := expression.NewCompiler(
    expression.WithNumberAdapter(decimalAdapter),
    expression.WithNumberAdapter(moneyAdapter),
)

runtimeGraph := runtimegraph.MustRuntimeGraph[OrderEvent](
    "order",
    runtimegraph.WithExpressionCompiler(compiler),
)
```

Internally, `WithNumberAdapter` registers the adapter in the converter,
comparator, and final bool converter chains.

## Evaluation Semantics

### Conversion

Expression variable lookup keeps the V1.2 lookup order:

1. runtime bag
2. configured provider
3. configured event resolver

Both ordinary `Variables.Lookup(path)` values and
`TypedVariables.LookupValue(path)` values must be able to reach number adapter
conversion. This closes the V1.2 gap where typed values bypassed custom
converters.

Adapter conversion can produce:

```go
Value{
    Kind:   ValueNumber,
    Number: decimalNumber,
}
```

The built-in converters still handle nil, bool, int, uint, float, and string
without requiring adapter involvement.

### Comparison

Built-in comparison remains:

- signed integer comparisons are exact.
- unsigned integer comparisons are exact.
- mixed signed/unsigned integer comparisons are exact.
- float comparisons use Go `float64` semantics.
- string comparisons remain lexicographic.
- bool supports `==` and `!=` only.
- nil supports `==` and `!=` only.

If built-in comparison cannot handle the pair, registered
`NumericComparator` implementations are tried. This includes:

- `ValueNumber` vs `ValueNumber`
- `ValueNumber` vs `ValueString`
- `ValueString` vs `ValueNumber`
- `ValueNumber` vs built-in numeric values
- `ValueObject` values that a custom converter first normalizes to
  `ValueNumber`

String-to-number parsing is adapter-owned behavior. The default engine does not
reinterpret strings as numbers.

### Final Bool Conversion

Number-to-bool conversion only applies to the final expression result.

Valid:

```text
${amount}
```

This may become `true` when the registered adapter treats the amount as non-zero.

Invalid unless the left side is already bool:

```text
${amount} && ${vip}
```

`&&`, `||`, and `!` keep V1.2 semantics: their operands must already be bool.

## Decimal Adapter Guidance

V1.3.0 includes tests and examples using a fake decimal-like type in the core
repository. The core module has no `github.com/shopspring/decimal` dependency.

Optional decimal adapters live outside the core hot path. Supported shapes:

- a separate package that users opt into, if accepting the dependency in the
  module is acceptable.
- a documentation recipe showing how to implement an adapter in application
  code.
- a separate module that keeps the root module free of optional decimal
  dependencies.

The V1.3.0 implementation uses fake adapter tests instead of a new third-party
dependency.

## Error Handling

Adapter errors propagate through the existing expression evaluation error path.
For runtime graph edges, adapter errors become condition failures and enter
`RuntimeGraphExceptionHandler[T]` with
`RuntimeGraphExceptionKindCondition`.

Examples:

- decimal string parse error in `${amount} > "10.50x"` returns an expression
  evaluation error.
- unsupported number pair returns the existing "cannot compare" style error.
- adapter bool conversion error on final result returns an expression evaluation
  error.

## Performance Requirements

Default behavior remains allocation-conscious:

- Runtime graph routes without expression conditions remain allocation-free.
- Default runtime graph expression branch benchmarks remain `0 allocs/op`.
- JSON tag typed resolver benchmark remains `0 allocs/op`.
- Adapter benchmarks report their own allocation profile separately
  because adapter implementations may allocate.

The built-in fast path must not route ordinary `int`, `uint`, and `float`
comparisons through adapter chains.

## Tests

Required tests:

- default `int`, `uint`, `float`, bool, string, and nil behavior remains
  compatible with V1.2.
- large integer comparison remains exact and does not fall back to float.
- `ValueNumber` can be produced by a custom adapter.
- custom adapter handles `ValueNumber` vs `ValueString`.
- custom adapter handles `ValueString` vs `ValueNumber`.
- custom adapter handles `ValueNumber` vs built-in numeric value.
- typed variables and ordinary variables both reach adapter conversion.
- multiple adapters execute by `Order()` ascending.
- equal adapter order keeps registration order.
- adapter error with `handled == true` stops evaluation.
- number-to-bool conversion applies only to the final expression result.
- number operands for `&&`, `||`, and `!` remain invalid.
- RuntimeGraph routes adapter comparison errors through condition exception
  handling.

Required benchmarks:

- default runtime graph single path
- default runtime graph expression branch
- default runtime graph active join
- typed resolver JSON tag lookup
- fake decimal adapter comparison path

## Documentation Updates

Update:

- `README.md`
- `README.zh-CN.md`
- `docs/api-guide.md`
- `examples/runtime_graph`
- benchmark docs if new adapter benchmarks are added

Docs state that decimal semantics are adapter-owned and outside the core
expression engine.

## Acceptance Criteria

- Public API exposes `NumberAdapter`, `OrderedNumberAdapter`, `Number`,
  `NumberKind`, and `ValueNumber`.
- Default V1.2 expression behavior is unchanged.
- Core module has no decimal dependency.
- Custom number adapters work for ordinary and typed variables.
- Adapter ordering is deterministic and documented.
- RuntimeGraph condition errors from adapters are recoverable through the
  existing exception path.
- Full tests, race tests, lint, and targeted benchmarks pass.
