# Benchmark Baseline

`main.txt` is the checked-in benchmark baseline for release comparison.

Refresh it only after a deliberate release-gate run:

```bash
go test -bench=. -benchmem -count=10 -cpu=1,2,4,8 ./... | tee benchmarks/baseline/main.txt
```

Compare a candidate branch against the baseline:

```bash
go test -bench=. -benchmem -count=10 -cpu=1,2,4,8 ./... | tee /tmp/disruptor-new.txt
benchstat benchmarks/baseline/main.txt /tmp/disruptor-new.txt
```

