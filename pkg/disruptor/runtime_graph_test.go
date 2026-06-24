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
	"sync"
	"testing"
	"time"

	"github.com/photowey/disruptor.go/pkg/disruptor"
)

func TestRuntimeGraphSnapshotAndValidationUseExplicitTerminals(t *testing.T) {
	graph := disruptor.MustRuntimeGraph[longEvent]("runtime").
		MustNode("A", runtimeRecordingHandler{}).
		MustNode("B", runtimeRecordingHandler{}).
		MustEdge(disruptor.GraphStartNode, "A").
		MustEdge("A", "B", disruptor.WhenExpression[longEvent](`${enabled}`)).
		MustEdge("B", disruptor.GraphEndNode)

	if err := graph.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	snapshot := graph.Snapshot()
	if got := snapshot.Entries; len(got) != 1 || got[0] != "A" {
		t.Fatalf("entries = %v, want [A]", got)
	}
	if got := snapshot.Exits; len(got) != 1 || got[0] != "B" {
		t.Fatalf("exits = %v, want [B]", got)
	}
	if got := snapshot.Edges[1].Condition; got != `${enabled}` {
		t.Fatalf("condition = %q, want expression", got)
	}
}

func TestDisruptorHandleRuntimeGraphRoutesExpressionPaths(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handled := make(chan string, 4)
	graph := disruptor.MustRuntimeGraph[longEvent]("runtime-route").
		MustNode("validate", runtimeRecordingHandler{
			name:    "validate",
			handled: handled,
			set: map[string]any{
				"route.even": true,
				"route.odd":  false,
			},
		}).
		MustNode("even", runtimeRecordingHandler{name: "even", handled: handled}).
		MustNode("odd", runtimeRecordingHandler{name: "odd", handled: handled}).
		MustEdge(disruptor.GraphStartNode, "validate").
		MustEdge("validate", "even", disruptor.WhenExpression[longEvent](`${route.even}`)).
		MustEdge("validate", "odd", disruptor.WhenExpression[longEvent](`${route.odd}`)).
		MustEdge("even", disruptor.GraphEndNode).
		MustEdge("odd", disruptor.GraphEndNode)

	d := newRuntimeGraphTestDisruptor(t, 8)
	if _, err := d.HandleRuntimeGraph(graph); err != nil {
		t.Fatalf("handle runtime graph: %v", err)
	}
	if err := d.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer d.Stop()

	publishValues(t, d.RingBuffer(), 2)
	wantRuntimeHandlers(t, handled, []string{"validate", "even"})
	assertNoRuntimeHandler(t, handled, "odd")
}

func TestRuntimeGraphActiveJoinExecutesNodeOnceWhenOneInboundSelected(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handled := make(chan string, 4)
	graph := disruptor.MustRuntimeGraph[longEvent]("runtime-join").
		MustNode("A", runtimeRecordingHandler{name: "A", handled: handled}).
		MustNode("B", runtimeRecordingHandler{name: "B", handled: handled}).
		MustNode("C", runtimeRecordingHandler{name: "C", handled: handled}).
		MustEdge(disruptor.GraphStartNode, "A", disruptor.WhenCondition[longEvent](runtimeBoolCondition(true))).
		MustEdge(disruptor.GraphStartNode, "B", disruptor.WhenCondition[longEvent](runtimeBoolCondition(false))).
		MustEdge("A", "C").
		MustEdge("B", "C").
		MustEdge("C", disruptor.GraphEndNode)

	d := newRuntimeGraphTestDisruptor(t, 8)
	if _, err := d.HandleRuntimeGraph(graph); err != nil {
		t.Fatalf("handle runtime graph: %v", err)
	}
	if err := d.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer d.Stop()

	publishValues(t, d.RingBuffer(), 1)
	wantRuntimeHandlers(t, handled, []string{"A", "C"})
	assertNoRuntimeHandler(t, handled, "B")
}

func TestRuntimeGraphNoRouteDefaultHalts(t *testing.T) {
	handled := make(chan string, 1)
	graph := disruptor.MustRuntimeGraph[longEvent]("runtime-no-route").
		MustNode("A", runtimeRecordingHandler{name: "A", handled: handled}).
		MustEdge(disruptor.GraphStartNode, "A", disruptor.WhenCondition[longEvent](runtimeBoolCondition(false))).
		MustEdge("A", disruptor.GraphEndNode)

	d := newRuntimeGraphTestDisruptor(t, 8)
	if _, err := d.HandleRuntimeGraph(graph); err != nil {
		t.Fatalf("handle runtime graph: %v", err)
	}
	if err := d.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	publishValues(t, d.RingBuffer(), 1)
	err := d.Wait()
	if !errors.Is(err, disruptor.ErrRuntimeNoRoute) {
		t.Fatalf("wait error = %v, want ErrRuntimeNoRoute", err)
	}
}

func TestRuntimeGraphNoRouteCompleteAdvancesSequence(t *testing.T) {
	graph := disruptor.MustRuntimeGraph[longEvent]("runtime-no-route-complete").
		MustNode("A", runtimeRecordingHandler{name: "A"}).
		MustEdge(disruptor.GraphStartNode, "A", disruptor.WhenCondition[longEvent](runtimeBoolCondition(false))).
		MustEdge("A", disruptor.GraphEndNode)

	d := newRuntimeGraphTestDisruptor(t, 1)
	processors, err := d.HandleRuntimeGraph(
		graph,
		disruptor.WithRuntimeGraphNoRouteAction[longEvent](
			disruptor.RuntimeNoRouteActionComplete,
		),
	)
	if err != nil {
		t.Fatalf("handle runtime graph: %v", err)
	}
	if err := d.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer d.Stop()

	publishValues(t, d.RingBuffer(), 1)
	waitForSequenceValue(t, processors.Sequence(), 0)
	d.Stop()
	if err := d.Wait(); err != nil {
		t.Fatalf("wait after no-route complete: %v", err)
	}
}

func TestRuntimeGraphMetricsSinkReceivesRoutingSignals(t *testing.T) {
	graph := disruptor.MustRuntimeGraph[longEvent]("runtime-metrics").
		MustNode("A", runtimeRecordingHandler{}).
		MustEdge(disruptor.GraphStartNode, "A").
		MustEdge("A", disruptor.GraphEndNode)

	sink := newRuntimeGraphMetricRecorder()
	d := newRuntimeGraphTestDisruptor(t, 8)
	processors, err := d.HandleRuntimeGraph(
		graph,
		disruptor.WithRuntimeGraphMetricsSink[longEvent](sink),
	)
	if err != nil {
		t.Fatalf("handle runtime graph: %v", err)
	}
	if err := d.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer d.Stop()

	publishValues(t, d.RingBuffer(), 1)
	waitForSequenceValue(t, processors.Sequence(), 0)

	for _, kind := range []string{"edge_selected", "node_scheduled", "node_completed", "complete"} {
		if !sink.has(kind) {
			t.Fatalf("missing runtime metric kind %q; got %v", kind, sink.kinds())
		}
	}
}

func TestRuntimeGraphExceptionHandlerReceivesFailures(t *testing.T) {
	t.Run("handler", func(t *testing.T) {
		handlerErr := errors.New("runtime handler failed")
		recorder := newRuntimeExceptionRecorder()
		graph := disruptor.MustRuntimeGraph[longEvent]("runtime-handler-error").
			MustNode("A", runtimeRecordingHandler{err: handlerErr}).
			MustEdge(disruptor.GraphStartNode, "A").
			MustEdge("A", disruptor.GraphEndNode)

		d := newRuntimeGraphTestDisruptor(t, 8)
		if _, err := d.HandleRuntimeGraph(
			graph,
			disruptor.WithRuntimeGraphExceptionHandler[longEvent](recorder),
		); err != nil {
			t.Fatalf("handle runtime graph: %v", err)
		}
		if err := d.Start(context.Background()); err != nil {
			t.Fatalf("start: %v", err)
		}

		publishValues(t, d.RingBuffer(), 1)
		if err := d.Wait(); !errors.Is(err, handlerErr) {
			t.Fatalf("wait error = %v, want handler error", err)
		}
		got := recorder.wait(t)
		if got.Kind != disruptor.RuntimeGraphExceptionKindHandler {
			t.Fatalf("exception kind = %v, want handler", got.Kind)
		}
		if got.NodeName != "A" {
			t.Fatalf("exception node = %q, want A", got.NodeName)
		}
	})

	t.Run("condition", func(t *testing.T) {
		conditionErr := errors.New("runtime condition failed")
		recorder := newRuntimeExceptionRecorder()
		graph := disruptor.MustRuntimeGraph[longEvent]("runtime-condition-error").
			MustNode("A", runtimeRecordingHandler{}).
			MustNode("B", runtimeRecordingHandler{}).
			MustEdge(disruptor.GraphStartNode, "A").
			MustEdge("A", "B", disruptor.WhenCondition[longEvent](
				runtimeErrorCondition{err: conditionErr},
			)).
			MustEdge("B", disruptor.GraphEndNode)

		d := newRuntimeGraphTestDisruptor(t, 8)
		if _, err := d.HandleRuntimeGraph(
			graph,
			disruptor.WithRuntimeGraphExceptionHandler[longEvent](recorder),
		); err != nil {
			t.Fatalf("handle runtime graph: %v", err)
		}
		if err := d.Start(context.Background()); err != nil {
			t.Fatalf("start: %v", err)
		}

		publishValues(t, d.RingBuffer(), 1)
		if err := d.Wait(); !errors.Is(err, conditionErr) {
			t.Fatalf("wait error = %v, want condition error", err)
		}
		got := recorder.wait(t)
		if got.Kind != disruptor.RuntimeGraphExceptionKindCondition {
			t.Fatalf("exception kind = %v, want condition", got.Kind)
		}
		if got.EdgeFrom != "A" || got.EdgeTo != "B" {
			t.Fatalf("exception edge = %s -> %s, want A -> B", got.EdgeFrom, got.EdgeTo)
		}
	})
}

func newRuntimeGraphTestDisruptor(t *testing.T, size int) *disruptor.Disruptor[longEvent] {
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

func wantRuntimeHandlers(t *testing.T, handled <-chan string, want []string) {
	t.Helper()

	for _, name := range want {
		select {
		case got := <-handled:
			if got != name {
				t.Fatalf("handler = %q, want %q", got, name)
			}
		case <-time.After(300 * time.Millisecond):
			t.Fatalf("timed out waiting for handler %q", name)
		}
	}
}

func assertNoRuntimeHandler(t *testing.T, handled <-chan string, name string) {
	t.Helper()

	select {
	case got := <-handled:
		if got == name {
			t.Fatalf("handler %q should have been skipped", name)
		}
		t.Fatalf("unexpected handler %q while checking %q", got, name)
	case <-time.After(50 * time.Millisecond):
	}
}

type runtimeRecordingHandler struct {
	name    string
	handled chan<- string
	set     map[string]any
	err     error
}

func (h runtimeRecordingHandler) OnEvent(request disruptor.EventRequest[longEvent]) error {
	if h.err != nil {
		return h.err
	}
	for path, value := range h.set {
		if err := request.Runtime.Set(path, value); err != nil {
			return err
		}
	}
	if h.handled != nil {
		h.handled <- h.name
	}

	return nil
}

type runtimeBoolCondition bool

func (c runtimeBoolCondition) Evaluate(
	request disruptor.EdgeConditionRequest[longEvent],
) (bool, error) {
	return bool(c), nil
}

type runtimeErrorCondition struct {
	err error
}

func (c runtimeErrorCondition) Evaluate(
	request disruptor.EdgeConditionRequest[longEvent],
) (bool, error) {
	return false, c.err
}

type runtimeExceptionRecorder struct {
	requests chan disruptor.RuntimeGraphExceptionRequest[longEvent]
}

func newRuntimeExceptionRecorder() runtimeExceptionRecorder {
	return runtimeExceptionRecorder{
		requests: make(chan disruptor.RuntimeGraphExceptionRequest[longEvent], 1),
	}
}

func (r runtimeExceptionRecorder) HandleRuntimeGraphException(
	request disruptor.RuntimeGraphExceptionRequest[longEvent],
) disruptor.ExceptionAction {
	r.requests <- request

	return disruptor.ExceptionActionHalt
}

func (r runtimeExceptionRecorder) wait(
	t *testing.T,
) disruptor.RuntimeGraphExceptionRequest[longEvent] {
	t.Helper()

	select {
	case request := <-r.requests:
		return request
	case <-time.After(300 * time.Millisecond):
		t.Fatal("timed out waiting for runtime graph exception")
	}

	return disruptor.RuntimeGraphExceptionRequest[longEvent]{}
}

type runtimeGraphMetricRecorder struct {
	mu      sync.Mutex
	metrics []disruptor.RuntimeGraphMetric
}

func newRuntimeGraphMetricRecorder() *runtimeGraphMetricRecorder {
	return &runtimeGraphMetricRecorder{}
}

func (r *runtimeGraphMetricRecorder) OnRuntimeGraph(
	request disruptor.RuntimeGraphMetric,
) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.metrics = append(r.metrics, request)
}

func (r *runtimeGraphMetricRecorder) has(kind string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, metric := range r.metrics {
		if metric.Kind == kind {
			return true
		}
	}

	return false
}

func (r *runtimeGraphMetricRecorder) kinds() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	kinds := make([]string, 0, len(r.metrics))
	for _, metric := range r.metrics {
		kinds = append(kinds, metric.Kind)
	}

	return kinds
}
