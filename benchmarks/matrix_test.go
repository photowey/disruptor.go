package benchmarks

import (
	"bufio"
	"os"
	"slices"
	"strings"
	"testing"
)

func TestBenchmarkMatrixCoversReleaseAxes(t *testing.T) {
	assertIntValues(t, "ring sizes", benchmarkRingSizes(), []int{1024, 65536, 1048576})
	assertStringValues(t, "payload shapes", benchmarkPayloadShapes(), []string{
		"small_value",
		"padded_value",
		"pointer_adapter",
	})
	assertStringValues(t, "topologies", benchmarkTopologies(), []string{
		"1p_1c",
		"1p_nc",
		"mp_1c",
		"mp_nc",
	})
	assertInt64Values(t, "batch sizes", benchmarkBatchSizes(), []int64{1, 4, 16, 64, 256})
	assertStringValues(t, "wait strategies", benchmarkWaitStrategyNames(), []string{
		"blocking",
		"busy_spin",
	})
}

func TestBenchmarkBaselineContainsStatisticalSamples(t *testing.T) {
	file, err := os.Open("baseline/baseline.txt")
	if err != nil {
		t.Fatalf("open baseline: %v", err)
	}
	defer file.Close()

	targets := map[string]map[string]bool{
		"BenchmarkE2EDisruptor/blocking_1p_1c":                   {},
		"BenchmarkRingBufferMatrix/ring_1024/small_value":        {},
		"BenchmarkRingBufferMatrix/ring_65536/padded_value":      {},
		"BenchmarkRingBufferMatrix/ring_1048576/pointer_adapter": {},
	}
	counts := make(map[string]int, len(targets))

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 0 || !strings.HasPrefix(fields[0], "Benchmark") {
			continue
		}
		for target := range targets {
			if benchmarkNameMatches(fields[0], target) {
				counts[target]++
				targets[target][benchmarkCPUSuffix(fields[0])] = true
			}
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan baseline: %v", err)
	}

	for target, cpus := range targets {
		if counts[target] < 10 {
			t.Fatalf("%s samples = %d, want at least 10", target, counts[target])
		}
		for _, cpu := range []string{"1", "2", "4", "8"} {
			if !cpus[cpu] {
				t.Fatalf("%s missing -cpu=%s sample", target, cpu)
			}
		}
	}
}

func assertIntValues(t *testing.T, name string, got, want []int) {
	t.Helper()

	if !slices.Equal(got, want) {
		t.Fatalf("%s = %v, want %v", name, got, want)
	}
}

func assertInt64Values(t *testing.T, name string, got, want []int64) {
	t.Helper()

	if !slices.Equal(got, want) {
		t.Fatalf("%s = %v, want %v", name, got, want)
	}
}

func assertStringValues(t *testing.T, name string, got, want []string) {
	t.Helper()

	if !slices.Equal(got, want) {
		t.Fatalf("%s = %v, want %v", name, got, want)
	}
}

func benchmarkCPUSuffix(name string) string {
	index := strings.LastIndex(name, "-")
	if index < 0 || index == len(name)-1 {
		return "1"
	}

	return name[index+1:]
}

func benchmarkNameMatches(name, target string) bool {
	return name == target || strings.HasPrefix(name, target+"-")
}
