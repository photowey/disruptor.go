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

package disruptor_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/photowey/disruptor.go/pkg/disruptor"
)

func TestDisruptorHandleGraphPipelineOrdersConsumers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d := newGraphTestDisruptor(t, 8)
	handled := make(chan string, 2)
	graph := disruptor.MustGraph[longEvent]("pipeline").
		MustNode("A", graphHandlerFunc(func(request disruptor.EventRequest[longEvent]) error {
			handled <- "A:" + request.Node.NodeName
			return nil
		})).
		MustNode("B", graphHandlerFunc(func(request disruptor.EventRequest[longEvent]) error {
			handled <- "B:" + request.Node.NodeName
			return nil
		})).
		MustEdge("A", "B")

	processors, err := d.HandleGraph(graph)
	if err != nil {
		t.Fatalf("handle graph: %v", err)
	}
	if names := processors.Names(); len(names) != 2 || names[0] != "A" || names[1] != "B" {
		t.Fatalf("processor names = %v, want [A B]", names)
	}

	if err := d.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer d.Stop()

	publishValues(t, d.RingBuffer(), 1)
	got := []string{
		waitForGraphHandler(t, handled),
		waitForGraphHandler(t, handled),
	}
	want := []string{"A:A", "B:B"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("handled[%d] = %q, want %q; all = %v", i, got[i], want[i], got)
		}
	}
}

func TestDisruptorHandleGraphJoinWaitsForAllUpstreamNodes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d := newGraphTestDisruptor(t, 8)
	releaseB := make(chan struct{})
	cHandled := make(chan struct{}, 1)
	graph := disruptor.MustGraph[longEvent]("join").
		MustNode("A", graphHandlerFunc(func(request disruptor.EventRequest[longEvent]) error {
			return nil
		})).
		MustNode("B", graphHandlerFunc(func(request disruptor.EventRequest[longEvent]) error {
			<-releaseB
			return nil
		})).
		MustNode("C", graphHandlerFunc(func(request disruptor.EventRequest[longEvent]) error {
			cHandled <- struct{}{}
			return nil
		}))
	graph.Join("A", "B").MustTo("C")

	if _, err := d.HandleGraph(graph); err != nil {
		t.Fatalf("handle graph: %v", err)
	}
	if err := d.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer d.Stop()

	publishValues(t, d.RingBuffer(), 1)
	select {
	case <-cHandled:
		t.Fatal("C handled before B advanced")
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseB)
	select {
	case <-cHandled:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timed out waiting for C after all upstream nodes advanced")
	}
}

func TestDisruptorHandleGraphOnlyLeavesGateProducers(t *testing.T) {
	d := newGraphTestDisruptor(t, 1)
	graph := disruptor.MustGraph[longEvent]("pipeline").
		MustNode("A", graphHandlerFunc(func(request disruptor.EventRequest[longEvent]) error {
			return nil
		})).
		MustNode("B", graphHandlerFunc(func(request disruptor.EventRequest[longEvent]) error {
			return nil
		})).
		MustEdge("A", "B")

	processors, err := d.HandleGraph(graph)
	if err != nil {
		t.Fatalf("handle graph: %v", err)
	}
	aSequence, ok := processors.Sequence("A")
	if !ok {
		t.Fatal("missing A sequence")
	}
	bSequence, ok := processors.Sequence("B")
	if !ok {
		t.Fatal("missing B sequence")
	}

	if _, err := d.RingBuffer().TryNext(); err != nil {
		t.Fatalf("first try next: %v", err)
	}
	if _, err := d.RingBuffer().TryNext(); !errors.Is(err, disruptor.ErrInsufficientCapacity) {
		t.Fatalf("second try next error = %v, want ErrInsufficientCapacity", err)
	}

	bSequence.Store(0)
	if _, err := d.RingBuffer().TryNext(); err != nil {
		t.Fatalf("try next after leaf sequence advanced: %v", err)
	}
	if got := aSequence.Value(); got != disruptor.InitialSequenceValue {
		t.Fatalf("A sequence = %d, want initial value to prove A did not gate", got)
	}
}

func TestDisruptorHandleGraphHaltStopsGraphWithoutAdvancingFailedSequence(t *testing.T) {
	d := newGraphTestDisruptor(t, 8)
	handlerErr := errors.New("graph handler failed")
	bHandled := make(chan struct{}, 1)
	graph := disruptor.MustGraph[longEvent]("halt").
		MustNode("A", graphHandlerFunc(func(request disruptor.EventRequest[longEvent]) error {
			return handlerErr
		})).
		MustNode("B", graphHandlerFunc(func(request disruptor.EventRequest[longEvent]) error {
			bHandled <- struct{}{}
			return nil
		})).
		MustEdge("A", "B")

	processors, err := d.HandleGraph(graph)
	if err != nil {
		t.Fatalf("handle graph: %v", err)
	}
	aSequence, ok := processors.Sequence("A")
	if !ok {
		t.Fatal("missing A sequence")
	}
	if err := d.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	publishValues(t, d.RingBuffer(), 1)
	if err := d.Wait(); !errors.Is(err, handlerErr) {
		t.Fatalf("wait error = %v, want handler error", err)
	}
	if got := aSequence.Value(); got != disruptor.InitialSequenceValue {
		t.Fatalf("A sequence = %d, want initial value", got)
	}
	select {
	case <-bHandled:
		t.Fatal("B should not handle a sequence that A failed with halt")
	default:
	}
}

func TestDisruptorHandleGraphUsesGraphExceptionHandler(t *testing.T) {
	d := newGraphTestDisruptor(t, 8)
	handlerErr := errors.New("ignored graph handler failure")
	graph := disruptor.MustGraph[longEvent]("ignore").
		MustNode("A", graphHandlerFunc(func(request disruptor.EventRequest[longEvent]) error {
			return handlerErr
		}))

	processors, err := d.HandleGraph(
		graph,
		disruptor.WithGraphExceptionHandler(
			disruptor.NewIgnoreExceptionHandler[longEvent](),
		),
	)
	if err != nil {
		t.Fatalf("handle graph: %v", err)
	}
	aSequence, ok := processors.Sequence("A")
	if !ok {
		t.Fatal("missing A sequence")
	}
	if err := d.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer d.Stop()

	publishValues(t, d.RingBuffer(), 1)
	waitForSequenceValue(t, aSequence, 0)
	d.Stop()
	if err := d.Wait(); err != nil {
		t.Fatalf("wait after ignored graph handler failure: %v", err)
	}
}

func TestDisruptorHandleGraphNodeExceptionHandlerOverridesGraphHandler(t *testing.T) {
	d := newGraphTestDisruptor(t, 8)
	handlerErr := errors.New("fatal node handler failure")
	graph := disruptor.MustGraph[longEvent]("node-override").
		MustNode(
			"A",
			graphHandlerFunc(func(request disruptor.EventRequest[longEvent]) error {
				return handlerErr
			}),
			disruptor.WithNodeExceptionHandler(
				disruptor.NewFatalExceptionHandler[longEvent](),
			),
		)

	processors, err := d.HandleGraph(
		graph,
		disruptor.WithGraphExceptionHandler(
			disruptor.NewIgnoreExceptionHandler[longEvent](),
		),
	)
	if err != nil {
		t.Fatalf("handle graph: %v", err)
	}
	aSequence, ok := processors.Sequence("A")
	if !ok {
		t.Fatal("missing A sequence")
	}
	if err := d.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	publishValues(t, d.RingBuffer(), 1)
	if err := d.Wait(); !errors.Is(err, handlerErr) {
		t.Fatalf("wait error = %v, want handler error", err)
	}
	if got := aSequence.Value(); got != disruptor.InitialSequenceValue {
		t.Fatalf("A sequence = %d, want initial value", got)
	}
}

func TestDisruptorHandleGraphRejectsFanOutModeConflict(t *testing.T) {
	d := newGraphTestDisruptor(t, 8)
	if _, err := d.HandleEventsWith(graphHandlerFunc(func(
		request disruptor.EventRequest[longEvent],
	) error {
		return nil
	})); err != nil {
		t.Fatalf("handle events with: %v", err)
	}

	_, err := d.HandleGraph(singleNodeGraph())
	if !errors.Is(err, disruptor.ErrConsumerModeConflict) {
		t.Fatalf("handle graph error = %v, want ErrConsumerModeConflict", err)
	}
}

func TestDisruptorHandleEventsWithRejectsGraphModeConflict(t *testing.T) {
	d := newGraphTestDisruptor(t, 8)
	if _, err := d.HandleGraph(singleNodeGraph()); err != nil {
		t.Fatalf("handle graph: %v", err)
	}

	_, err := d.HandleEventsWith(graphHandlerFunc(func(
		request disruptor.EventRequest[longEvent],
	) error {
		return nil
	}))
	if !errors.Is(err, disruptor.ErrConsumerModeConflict) {
		t.Fatalf("handle events with error = %v, want ErrConsumerModeConflict", err)
	}
}

func TestDisruptorHandleGraphRejectsStartedDisruptor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d := newGraphTestDisruptor(t, 8)
	if err := d.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer d.Stop()

	_, err := d.HandleGraph(singleNodeGraph())
	if !errors.Is(err, disruptor.ErrClosed) {
		t.Fatalf("handle graph error = %v, want ErrClosed", err)
	}
}

func TestDisruptorHandleGraphRejectsHandledGraph(t *testing.T) {
	graph := singleNodeGraph()
	first := newGraphTestDisruptor(t, 8)
	if _, err := first.HandleGraph(graph); err != nil {
		t.Fatalf("first handle graph: %v", err)
	}

	second := newGraphTestDisruptor(t, 8)
	_, err := second.HandleGraph(graph)
	if !errors.Is(err, disruptor.ErrGraphHandled) {
		t.Fatalf("second handle graph error = %v, want ErrGraphHandled", err)
	}
}

func TestGraphProcessorsLookupAndSnapshot(t *testing.T) {
	d := newGraphTestDisruptor(t, 8)
	graph := disruptor.MustGraph[longEvent]("lookup").
		MustNode("B", graphHandlerFunc(func(request disruptor.EventRequest[longEvent]) error {
			return nil
		})).
		MustNode("A", graphHandlerFunc(func(request disruptor.EventRequest[longEvent]) error {
			return nil
		})).
		MustEdge("A", "B")

	processors, err := d.HandleGraph(graph)
	if err != nil {
		t.Fatalf("handle graph: %v", err)
	}
	if names := processors.Names(); len(names) != 2 || names[0] != "A" || names[1] != "B" {
		t.Fatalf("names = %v, want [A B]", names)
	}
	if _, ok := processors.Processor("A"); !ok {
		t.Fatal("missing processor A")
	}
	if _, ok := processors.Processor("missing"); ok {
		t.Fatal("unexpected missing processor")
	}
	if _, ok := processors.Sequence("A"); !ok {
		t.Fatal("missing sequence A")
	}
	if _, ok := processors.Sequence("missing"); ok {
		t.Fatal("unexpected missing sequence")
	}

	snapshot := processors.Snapshot()
	if !snapshot.Frozen {
		t.Fatal("snapshot should be frozen after HandleGraph")
	}
	if names := processors.Names(); len(names) != 2 || names[0] != "A" || names[1] != "B" {
		t.Fatalf("processor names = %v, want only real nodes [A B]", names)
	}
	if !graphSnapshotHasNode(snapshot, disruptor.GraphStartNode) {
		t.Fatalf("snapshot nodes = %+v, want virtual START", snapshot.Nodes)
	}
	if !graphSnapshotHasNode(snapshot, disruptor.GraphEndNode) {
		t.Fatalf("snapshot nodes = %+v, want virtual END", snapshot.Nodes)
	}
	snapshot.Nodes[0].Name = "changed"
	if fresh := processors.Snapshot(); fresh.Nodes[0].Name != disruptor.GraphStartNode {
		t.Fatalf("snapshot mutation leaked, got %q", fresh.Nodes[0].Name)
	}
}

func newGraphTestDisruptor(t *testing.T, size int) *disruptor.Disruptor[longEvent] {
	t.Helper()

	d, err := disruptor.New(
		disruptor.EventFactoryFunc[longEvent](func() longEvent { return longEvent{} }),
		size,
	)
	if err != nil {
		t.Fatalf("new disruptor: %v", err)
	}

	return d
}

func waitForGraphHandler(t *testing.T, handled <-chan string) string {
	t.Helper()

	select {
	case value := <-handled:
		return value
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timed out waiting for graph handler")
	}

	return ""
}

func waitForSequenceValue(t *testing.T, sequence *disruptor.Sequence, want int64) {
	t.Helper()

	deadline := time.After(200 * time.Millisecond)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			t.Fatalf("sequence = %d, want %d", sequence.Value(), want)
		case <-ticker.C:
			if sequence.Value() == want {
				return
			}
		}
	}
}

func singleNodeGraph() *disruptor.Graph[longEvent] {
	return disruptor.MustGraph[longEvent]("single").
		MustNode("only", graphHandlerFunc(func(
			request disruptor.EventRequest[longEvent],
		) error {
			return nil
		}))
}

func graphSnapshotHasNode(snapshot disruptor.GraphSnapshot, name string) bool {
	for _, node := range snapshot.Nodes {
		if node.Name == name {
			return true
		}
	}

	return false
}

type graphHandlerFunc func(disruptor.EventRequest[longEvent]) error

func (fn graphHandlerFunc) OnEvent(request disruptor.EventRequest[longEvent]) error {
	return fn(request)
}
