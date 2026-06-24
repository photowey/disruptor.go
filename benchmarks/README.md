# Benchmarks

Benchmarks are part of release readiness for this repository.

Run the full local benchmark suite:

```bash
go test -run '^$' -bench=. -benchmem -benchtime=100ms -count=10 ./...
```

Run only end-to-end, topology, and comparison groups:

```bash
go test -run '^$' -bench=BenchmarkE2E -benchmem -count=10 ./benchmarks
go test -run '^$' -bench=BenchmarkE2EDisruptorParallelProducers -benchmem -count=10 ./benchmarks
go test -run '^$' -bench=BenchmarkChannelComparison -benchmem -count=10 ./benchmarks
go test -run '^$' -bench=BenchmarkGraphTopology -benchmem -count=10 ./benchmarks
go test -run '^$' -bench=BenchmarkRuntimeGraphRouting -benchmem -count=10 ./benchmarks
go test -run '^$' -bench=BenchmarkE2ELatencyQuantiles -benchmem -count=10 ./benchmarks
```

Recommended release comparison:

```bash
go test -run '^$' -bench=. -benchmem -benchtime=100ms -count=10 -cpu=1,2,4,8 ./... | tee /tmp/disruptor-new.txt
benchstat benchmarks/baseline/baseline.txt /tmp/disruptor-new.txt
```

The checked-in baseline lives in `benchmarks/baseline/baseline.txt`.
Regenerate it only after an intentional release-gate run on the target machine.

Benchmark matrix:

| Axis | Current groups |
| --- | --- |
| Ring size | `1024`, `65536`, `1048576` in `BenchmarkRingBufferMatrix`; `65536` in end-to-end and channel comparisons |
| Topology | `1/1`, `1/N`, `M/1`, `M/N`, graph single-node, graph pipeline, graph fan-in, graph diamond, runtime graph single-path, runtime graph expression branch, and runtime graph active join |
| Wait strategy | blocking and busy-spin |
| Claim batch size | `1`, `4`, `16`, `64`, `256` |
| Queue comparison | unbuffered channel, buffered channel, pointer channel, spin channel, `sync.Cond` ring |
| CPU matrix | run release gate with `-cpu=1,2,4,8` |

`BenchmarkE2ELatencyQuantiles` reports sampled publish-to-handle `p50_ns`,
`p95_ns`, and `p99_ns` for blocking and busy-spin wait strategies. Treat these
tail-latency metrics as release gates alongside `ns/op`, `B/op`, `allocs/op`,
and `events/s`.

Use channel benchmarks as context, not as a claim that one primitive replaces
the other. Channels remain the default Go tool for ordinary ownership transfer.
Disruptor is intended for measured high-throughput, low-allocation, fan-out, and
controlled-backpressure workflows.
