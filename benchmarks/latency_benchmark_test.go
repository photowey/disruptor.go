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

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/photowey/disruptor.go/pkg/disruptor"
)

const latencySampleLimit = 4096

type latencyEvent struct {
	PublishedAt time.Time
}

func BenchmarkE2ELatencyQuantiles(b *testing.B) {
	for _, tt := range []struct {
		name         string
		waitStrategy disruptor.WaitStrategy
	}{
		{
			name:         "blocking_1p_1c",
			waitStrategy: disruptor.NewBlockingWaitStrategy(),
		},
		{
			name:         "busy_spin_1p_1c",
			waitStrategy: disruptor.NewBusySpinWaitStrategy(),
		},
	} {
		b.Run(tt.name, func(b *testing.B) {
			benchmarkE2ELatencyQuantiles(b, tt.waitStrategy)
		})
	}
}

func benchmarkE2ELatencyQuantiles(
	b *testing.B,
	waitStrategy disruptor.WaitStrategy,
) {
	b.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d, err := disruptor.New(
		disruptor.EventFactoryFunc[latencyEvent](func() latencyEvent {
			return latencyEvent{}
		}),
		65536,
		disruptor.WithWaitStrategy(waitStrategy),
	)
	if err != nil {
		b.Fatalf("new disruptor: %v", err)
	}

	var processed atomic.Int64
	sampleEvery := latencySampleEvery(b.N)
	latencies := make([]int64, 0, latencySampleLimit)
	var latenciesMu sync.Mutex
	handler := disruptor.EventHandlerFunc[latencyEvent](func(
		request disruptor.EventRequest[latencyEvent],
	) error {
		count := processed.Add(1)
		if (count-1)%sampleEvery == 0 {
			latency := time.Since(request.Event.PublishedAt).Nanoseconds()
			latenciesMu.Lock()
			if len(latencies) < latencySampleLimit {
				latencies = append(latencies, latency)
			}
			latenciesMu.Unlock()
		}

		return nil
	})
	if _, err := d.HandleEventsWith(handler); err != nil {
		b.Fatalf("handle events with: %v", err)
	}
	if err := d.Start(ctx); err != nil {
		b.Fatalf("start disruptor: %v", err)
	}

	started := time.Now()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		sequence, err := d.RingBuffer().Next(ctx)
		if err != nil {
			b.Fatalf("next: %v", err)
		}
		d.RingBuffer().Get(sequence).PublishedAt = time.Now()
		d.RingBuffer().Publish(sequence)
	}
	b.StopTimer()

	waitForBenchmarkEvents(b, &processed, int64(b.N))

	d.Stop()
	if err := d.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		b.Fatalf("wait disruptor: %v", err)
	}

	latenciesMu.Lock()
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})
	reportLatencyQuantiles(b, latencies)
	latenciesMu.Unlock()

	elapsed := time.Since(started).Seconds()
	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed, "events/s")
	}
}

func latencySampleEvery(iterations int) int64 {
	if iterations <= latencySampleLimit {
		return 1
	}

	stride := iterations / latencySampleLimit
	if stride < 1 {
		return 1
	}

	return int64(stride)
}

func reportLatencyQuantiles(b *testing.B, sortedLatencies []int64) {
	b.Helper()

	if len(sortedLatencies) == 0 {
		return
	}

	b.ReportMetric(float64(latencyPercentile(sortedLatencies, 0.50)), "p50_ns")
	b.ReportMetric(float64(latencyPercentile(sortedLatencies, 0.95)), "p95_ns")
	b.ReportMetric(float64(latencyPercentile(sortedLatencies, 0.99)), "p99_ns")
}

func latencyPercentile(sortedLatencies []int64, quantile float64) int64 {
	if len(sortedLatencies) == 1 {
		return sortedLatencies[0]
	}

	index := int(float64(len(sortedLatencies)-1) * quantile)
	return sortedLatencies[index]
}
