SHELL := /bin/sh

.DEFAULT_GOAL := help

GO ?= go
GIT ?= git
GOFMT ?= gofmt
GOLANGCI_LINT ?= golangci-lint
BENCHSTAT ?= benchstat

COVERPROFILE ?= .tmp/coverage.out
BENCH_OUTPUT ?= bench.txt
BENCH ?= .
BENCHTIME ?= 100ms
BENCHCOUNT ?= 10
BENCHCPU ?= 1,2,4,8
LINT_TIMEOUT ?= 10m

.PHONY: help
help:
	@printf '%s\n' 'Disruptor.go development targets:'
	@printf '%s\n' '  make ci             run local release-readiness checks'
	@printf '%s\n' '  make test           run tests with shuffle and coverage'
	@printf '%s\n' '  make race           run race tests'
	@printf '%s\n' '  make vet            run go vet'
	@printf '%s\n' '  make lint           run golangci-lint'
	@printf '%s\n' '  make fmt            format Go packages'
	@printf '%s\n' '  make fmt-check      verify gofmt output'
	@printf '%s\n' '  make examples       run executable examples'
	@printf '%s\n' '  make bench-smoke    run the CI benchmark smoke test'
	@printf '%s\n' '  make bench          run the local benchmark suite'
	@printf '%s\n' '  make bench-release  run CPU-matrix benchmarks and benchstat'
	@printf '%s\n' '  make clean          remove generated local artifacts'

.PHONY: ci
ci: tidy-check fmt-check test race vet lint bench-smoke

.PHONY: mod-download
mod-download:
	$(GO) mod download

.PHONY: tidy
tidy:
	$(GO) mod tidy

.PHONY: tidy-check
tidy-check:
	$(GO) mod tidy
	$(GIT) diff --exit-code -- go.mod go.sum

.PHONY: fmt
fmt:
	$(GO) fmt ./...

.PHONY: fmt-check
fmt-check:
	@files="$$($(GOFMT) -l .)"; \
	if [ -n "$$files" ]; then \
		printf '%s\n' 'The following Go files are not formatted:'; \
		printf '%s\n' "$$files"; \
		exit 1; \
	fi

.PHONY: test
test:
	mkdir -p .tmp
	$(GO) test -shuffle=on -coverprofile=$(COVERPROFILE) ./...

.PHONY: coverage
coverage: test
	$(GO) tool cover -func=$(COVERPROFILE)

.PHONY: race
race:
	$(GO) test -race -shuffle=on ./...

.PHONY: vet
vet:
	$(GO) vet ./...

.PHONY: lint
lint:
	$(GOLANGCI_LINT) run --timeout=$(LINT_TIMEOUT)

.PHONY: examples
examples:
	$(GO) test ./examples/...

.PHONY: bench-smoke
bench-smoke:
	$(GO) test -run '^$$' -bench='Benchmark(RingBufferMatrix|ExecutorSubmitInline|RuntimeGraphRoutingParallel)' -benchmem -benchtime=100ms -count=1 ./benchmarks

.PHONY: bench
bench:
	$(GO) test -run '^$$' -bench=$(BENCH) -benchmem -benchtime=$(BENCHTIME) -count=$(BENCHCOUNT) ./...

.PHONY: bench-release
bench-release:
	$(GO) test -run '^$$' -bench=$(BENCH) -benchmem -benchtime=$(BENCHTIME) -count=$(BENCHCOUNT) -cpu=$(BENCHCPU) ./... | tee $(BENCH_OUTPUT)
	$(BENCHSTAT) benchmarks/baseline/baseline.txt $(BENCH_OUTPUT)

.PHONY: clean
clean:
	rm -rf coverage.out coverage.html *.test bench.txt
