// Copyright © 2026-present The Disruptor.go Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package benchmarks

import "github.com/photowey/disruptor.go/pkg/wait"

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

type benchmarkWaitStrategyFactory func() wait.Strategy

func benchmarkWaitStrategyCases() []benchmarkWaitStrategyCase {
	return []benchmarkWaitStrategyCase{
		{
			name:    "blocking",
			factory: wait.NewBlockingStrategy,
		},
		{
			name:    "busy_spin",
			factory: wait.NewBusySpinStrategy,
		},
	}
}

type paddedBenchEvent struct {
	Value int64
	_     [7]int64
}
