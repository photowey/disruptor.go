# Benchmarks

Benchmarks are part of V1 release readiness for this repository.

Run the full local benchmark suite:

```bash
go test -bench=. -benchmem -count=10 ./...
```

Run only end-to-end and comparison groups:

```bash
go test -bench=BenchmarkE2E -benchmem -count=10 ./benchmarks
go test -bench=BenchmarkChannelComparison -benchmem -count=10 ./benchmarks
```

Recommended release comparison:

```bash
go test -bench=. -benchmem -count=10 -cpu=1,2,4,8 ./... | tee /tmp/disruptor-new.txt
benchstat /tmp/disruptor-old.txt /tmp/disruptor-new.txt
```

Use channel benchmarks as context, not as a claim that one primitive replaces
the other. Channels remain the default Go tool for ordinary ownership transfer.
Disruptor is intended for measured high-throughput, low-allocation, fan-out, and
controlled-backpressure workflows.

