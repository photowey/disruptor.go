package benchmarks

import "github.com/photowey/disruptor.go/pkg/disruptor"

func benchmarkRingSizes() []int {
	return []int{1024, 65536, 1048576}
}

func benchmarkPayloadShapes() []string {
	return []string{
		"small_value",
		"padded_value",
		"pointer_adapter",
	}
}

func benchmarkTopologies() []string {
	return []string{
		"1p_1c",
		"1p_nc",
		"mp_1c",
		"mp_nc",
	}
}

func benchmarkBatchSizes() []int64 {
	return []int64{1, 4, 16, 64, 256}
}

func benchmarkWaitStrategyNames() []string {
	cases := benchmarkWaitStrategyCases()
	names := make([]string, 0, len(cases))
	for _, item := range cases {
		names = append(names, item.name)
	}

	return names
}

type benchmarkWaitStrategyCase struct {
	name    string
	factory benchmarkWaitStrategyFactory
}

type benchmarkWaitStrategyFactory func() disruptor.WaitStrategy

func benchmarkWaitStrategyCases() []benchmarkWaitStrategyCase {
	return []benchmarkWaitStrategyCase{
		{
			name:    "blocking",
			factory: disruptor.NewBlockingWaitStrategy,
		},
		{
			name:    "busy_spin",
			factory: disruptor.NewBusySpinWaitStrategy,
		},
	}
}

type paddedBenchEvent struct {
	Value int64
	_     [7]int64
}
