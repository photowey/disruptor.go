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
	"sync/atomic"
	"testing"

	"github.com/photowey/disruptor.go/pkg/disruptor"
)

func BenchmarkGraphTopology(b *testing.B) {
	for _, shape := range []string{"single_node", "pipeline", "fan_in", "diamond"} {
		b.Run(shape, func(b *testing.B) {
			benchmarkGraphTopology(b, shape)
		})
	}
}

func benchmarkGraphTopology(b *testing.B, shape string) {
	b.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d, err := disruptor.New(
		disruptor.EventFactoryFunc[benchEvent](func() benchEvent { return benchEvent{} }),
		65536,
	)
	if err != nil {
		b.Fatalf("new disruptor: %v", err)
	}

	var processed atomic.Int64
	graph, handlerCount := newBenchmarkGraph(shape, &processed)
	if _, err := d.HandleGraph(graph); err != nil {
		b.Fatalf("handle graph: %v", err)
	}
	if err := d.Start(ctx); err != nil {
		b.Fatalf("start disruptor: %v", err)
	}

	publishContext := context.Background()
	var published int64
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		sequence, err := d.RingBuffer().Next(publishContext)
		if err != nil {
			b.Fatalf("next: %v", err)
		}
		d.RingBuffer().Get(sequence).Value = published
		d.RingBuffer().Publish(sequence)
		published++
	}
	elapsed := b.Elapsed().Seconds()
	b.StopTimer()

	target := published * int64(handlerCount)
	waitForBenchmarkEvents(b, &processed, target)

	d.Stop()
	if err := d.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		b.Fatalf("wait disruptor: %v", err)
	}

	if elapsed > 0 {
		b.ReportMetric(float64(published)/elapsed, "events/s")
	}
}

func newBenchmarkGraph(
	shape string,
	processed *atomic.Int64,
) (*disruptor.Graph[benchEvent], int) {
	handler := graphBenchHandler{processed: processed}
	switch shape {
	case "single_node":
		return disruptor.MustGraph[benchEvent]("single-node").
			MustNode("A", handler), 1
	case "pipeline":
		return disruptor.MustGraph[benchEvent]("pipeline").
			MustNode("A", handler).
			MustNode("B", handler).
			MustNode("C", handler).
			MustEdge("A", "B").
			MustEdge("B", "C"), 3
	case "fan_in":
		graph := disruptor.MustGraph[benchEvent]("fan-in").
			MustNode("A", handler).
			MustNode("B", handler).
			MustNode("C", handler)
		graph.Join("A", "B").MustTo("C")
		return graph, 3
	case "diamond":
		graph := disruptor.MustGraph[benchEvent]("diamond").
			MustNode("A", handler).
			MustNode("B", handler).
			MustNode("C", handler).
			MustNode("D", handler).
			MustEdge("A", "B").
			MustEdge("A", "C")
		graph.Join("B", "C").MustTo("D")
		return graph, 4
	default:
		panic("unknown graph benchmark shape: " + shape)
	}
}

type graphBenchHandler struct {
	processed *atomic.Int64
}

func (h graphBenchHandler) OnEvent(request disruptor.EventRequest[benchEvent]) error {
	benchmarkValueSink.Store(request.Event.Value)
	h.processed.Add(1)
	return nil
}
