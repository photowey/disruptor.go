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
	topology "github.com/photowey/disruptor.go/pkg/graph"
	"github.com/photowey/disruptor.go/pkg/runtimegraph"
	"sync/atomic"
	"testing"

	"github.com/photowey/disruptor.go/pkg/disruptor"
)

func BenchmarkRuntimeGraphRouting(b *testing.B) {
	for _, shape := range []string{"single_path", "expression_branch", "active_join"} {
		b.Run(shape, func(b *testing.B) {
			benchmarkRuntimeGraphRouting(b, shape, 1)
		})
	}
}

func BenchmarkRuntimeGraphRoutingParallel(b *testing.B) {
	benchmarkRuntimeGraphRouting(b, "parallel_workers", 2)
}

func benchmarkRuntimeGraphRouting(b *testing.B, shape string, workers int) {
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
	graph, handlerCount := newBenchmarkRuntimeGraph(shape, &processed)
	if _, err := d.HandleRuntimeGraph(
		graph,
		disruptor.WithRuntimeGraphWorkers[benchEvent](workers),
	); err != nil {
		b.Fatalf("handle runtime graph: %v", err)
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

func newBenchmarkRuntimeGraph(
	shape string,
	processed *atomic.Int64,
) (*runtimegraph.RuntimeGraph[benchEvent], int) {
	handler := graphBenchHandler{processed: processed}
	switch shape {
	case "single_path":
		return runtimegraph.MustRuntimeGraph[benchEvent]("runtime-single").
			MustNode("A", handler).
			MustEdge(topology.StartNode, "A").
			MustEdge("A", topology.EndNode), 1
	case "expression_branch":
		return runtimegraph.MustRuntimeGraph[benchEvent]("runtime-expression").
			MustNode("route", handler).
			MustNode("fast", handler).
			MustNode("audit", handler).
			MustEdge(topology.StartNode, "route").
			MustEdge("route", "fast", runtimegraph.WhenExpression[benchEvent](`${value} >= 0`)).
			MustEdge("route", "audit", runtimegraph.WhenExpression[benchEvent](`${value} < 0`)).
			MustEdge("fast", topology.EndNode).
			MustEdge("audit", topology.EndNode), 2
	case "active_join":
		graph := runtimegraph.MustRuntimeGraph[benchEvent]("runtime-join").
			MustNode("A", handler).
			MustNode("B", handler).
			MustNode("C", handler).
			MustEdge(
				topology.StartNode,
				"A",
				runtimegraph.WhenCondition[benchEvent](benchRuntimeCondition(true)),
			).
			MustEdge(
				topology.StartNode,
				"B",
				runtimegraph.WhenCondition[benchEvent](benchRuntimeCondition(false)),
			).
			MustEdge("A", "C").
			MustEdge("B", "C").
			MustEdge("C", topology.EndNode)
		return graph, 2
	case "parallel_workers":
		graph := runtimegraph.MustRuntimeGraph[benchEvent]("runtime-parallel").
			MustNode("A", handler).
			MustNode("B", handler).
			MustNode("C", handler).
			MustEdge(topology.StartNode, "A").
			MustEdge(topology.StartNode, "B").
			MustEdge("A", "C").
			MustEdge("B", "C").
			MustEdge("C", topology.EndNode)
		return graph, 3
	default:
		panic("unknown runtime graph benchmark shape: " + shape)
	}
}

type benchRuntimeCondition bool

func (c benchRuntimeCondition) Evaluate(
	request runtimegraph.EdgeConditionRequest[benchEvent],
) (bool, error) {
	return bool(c), nil
}
