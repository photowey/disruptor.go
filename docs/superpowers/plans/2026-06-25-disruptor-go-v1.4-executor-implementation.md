# Disruptor.go V1.4 External Pool Integration Plan

## Objective

Remove the local general-purpose executor package from disruptor.go and use
`github.com/photowey/pool.go/pkg/pool` for RuntimeGraph external execution.

## File Structure

- Remove the local general-purpose executor package.
- Remove the standalone executor example.
- Remove executor-only benchmarks.
- Keep `examples/runtime_graph_executor` as the RuntimeGraph external pool
  example.
- Update RuntimeGraph executor wiring in `pkg/disruptor`.
- Update README, Chinese README, API guide, and benchmark documentation.

## RuntimeGraph Integration

- `WithRuntimeGraphExecutor[T]` accepts `pool.Executor`.
- `WithRuntimeGraphWorkers[T]` creates an internal fixed pool when workers are
  greater than one.
- Supplying both workers and an external executor remains invalid.
- Caller-owned executors are not shut down by Disruptor.
- Internal executors are shut down by Disruptor during processor halt.
- RuntimeGraph node tasks report queued cancellation through the same completion
  envelope as executed tasks.

## Validation

- `go mod tidy`
- `go test ./...`
- `go test -race ./pkg/disruptor`
- `go test -run '^$' -bench='Benchmark(RingBufferMatrix|RuntimeGraphRoutingParallel)' -benchmem -benchtime=100ms -count=1 ./benchmarks`
- `make ci`
- Run a repository-wide legacy executor reference scan.
