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
	"sync/atomic"
	"testing"
	"time"

	"github.com/photowey/disruptor.go/pkg/disruptor"
	"github.com/photowey/disruptor.go/pkg/event"
	"github.com/photowey/disruptor.go/pkg/executor"
	"github.com/photowey/disruptor.go/pkg/expression"
	topology "github.com/photowey/disruptor.go/pkg/graph"
	"github.com/photowey/disruptor.go/pkg/runtimegraph"
)

func TestRuntimeGraphSnapshotAndValidationUseExplicitTerminals(t *testing.T) {
	graph := runtimegraph.MustRuntimeGraph[longEvent]("runtime").
		MustNode("A", runtimeRecordingHandler{}).
		MustNode("B", runtimeRecordingHandler{}).
		MustEdge(topology.StartNode, "A").
		MustEdge("A", "B", runtimegraph.WhenExpression[longEvent](`${enabled}`)).
		MustEdge("B", topology.EndNode)

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
	graph := runtimegraph.MustRuntimeGraph[longEvent]("runtime-route").
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
		MustEdge(topology.StartNode, "validate").
		MustEdge("validate", "even", runtimegraph.WhenExpression[longEvent](`${route.even}`)).
		MustEdge("validate", "odd", runtimegraph.WhenExpression[longEvent](`${route.odd}`)).
		MustEdge("even", topology.EndNode).
		MustEdge("odd", topology.EndNode)

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
	graph := runtimegraph.MustRuntimeGraph[longEvent]("runtime-join").
		MustNode("A", runtimeRecordingHandler{name: "A", handled: handled}).
		MustNode("B", runtimeRecordingHandler{name: "B", handled: handled}).
		MustNode("C", runtimeRecordingHandler{name: "C", handled: handled}).
		MustEdge(topology.StartNode, "A", runtimegraph.WhenCondition[longEvent](runtimeBoolCondition(true))).
		MustEdge(topology.StartNode, "B", runtimegraph.WhenCondition[longEvent](runtimeBoolCondition(false))).
		MustEdge("A", "C").
		MustEdge("B", "C").
		MustEdge("C", topology.EndNode)

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
	graph := runtimegraph.MustRuntimeGraph[longEvent]("runtime-no-route").
		MustNode("A", runtimeRecordingHandler{name: "A", handled: handled}).
		MustEdge(topology.StartNode, "A", runtimegraph.WhenCondition[longEvent](runtimeBoolCondition(false))).
		MustEdge("A", topology.EndNode)

	d := newRuntimeGraphTestDisruptor(t, 8)
	if _, err := d.HandleRuntimeGraph(graph); err != nil {
		t.Fatalf("handle runtime graph: %v", err)
	}
	if err := d.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	publishValues(t, d.RingBuffer(), 1)
	err := d.Wait()
	if !errors.Is(err, runtimegraph.ErrNoRoute) {
		t.Fatalf("wait error = %v, want runtimegraph.ErrNoRoute", err)
	}
}

func TestRuntimeGraphNoRouteCompleteAdvancesSequence(t *testing.T) {
	graph := runtimegraph.MustRuntimeGraph[longEvent]("runtime-no-route-complete").
		MustNode("A", runtimeRecordingHandler{name: "A"}).
		MustEdge(topology.StartNode, "A", runtimegraph.WhenCondition[longEvent](runtimeBoolCondition(false))).
		MustEdge("A", topology.EndNode)

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
	graph := runtimegraph.MustRuntimeGraph[longEvent]("runtime-metrics").
		MustNode("A", runtimeRecordingHandler{}).
		MustEdge(topology.StartNode, "A").
		MustEdge("A", topology.EndNode)

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
		graph := runtimegraph.MustRuntimeGraph[longEvent]("runtime-handler-error").
			MustNode("A", runtimeRecordingHandler{err: handlerErr}).
			MustEdge(topology.StartNode, "A").
			MustEdge("A", topology.EndNode)

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
		graph := runtimegraph.MustRuntimeGraph[longEvent]("runtime-condition-error").
			MustNode("A", runtimeRecordingHandler{}).
			MustNode("B", runtimeRecordingHandler{}).
			MustEdge(topology.StartNode, "A").
			MustEdge("A", "B", runtimegraph.WhenCondition[longEvent](
				runtimeErrorCondition{err: conditionErr},
			)).
			MustEdge("B", topology.EndNode)

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

func TestRuntimeGraphNodeExceptionHandlerOverridesRuntimeHandlerForHandlerErrors(t *testing.T) {
	retryHandler, err := event.NewRetryExceptionHandler[longEvent](
		1,
		event.ExceptionActionHalt,
	)
	if err != nil {
		t.Fatalf("new retry handler: %v", err)
	}

	graph := runtimegraph.MustRuntimeGraph[longEvent]("runtime-node-handler-override").
		MustNode(
			"A",
			&runtimeFailOnceHandler{err: errors.New("ignored once")},
			runtimegraph.WithNodeExceptionHandler[longEvent](retryHandler),
		).
		MustEdge(topology.StartNode, "A").
		MustEdge("A", topology.EndNode)

	recorder := newRuntimeExceptionRecorder()
	d := newRuntimeGraphTestDisruptor(t, 8)
	processors, err := d.HandleRuntimeGraph(
		graph,
		disruptor.WithRuntimeGraphExceptionHandler[longEvent](recorder),
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
		t.Fatalf("wait after node-level retry: %v", err)
	}
	recorder.assertNoRequest(t)
}

func TestRuntimeGraphReportsNumberAdapterConditionErrors(t *testing.T) {
	conditionErr := errors.New("runtime decimal compare failed")
	compiler := expression.NewCompiler(
		expression.WithNumberAdapter(runtimeDecimalAdapter{err: conditionErr}),
	)
	recorder := newRuntimeExceptionRecorder()
	graph := runtimegraph.MustRuntimeGraph[longEvent](
		"runtime-number-adapter-error",
		runtimegraph.WithExpressionCompiler(compiler),
	).
		MustNode("A", runtimeRecordingHandler{
			set: map[string]any{
				"amount": runtimeDecimalRaw{cents: 1125},
			},
		}).
		MustNode("B", runtimeRecordingHandler{}).
		MustEdge(topology.StartNode, "A").
		MustEdge("A", "B", runtimegraph.WhenExpression[longEvent](`${amount} > "bad"`)).
		MustEdge("B", topology.EndNode)

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
}

func TestRuntimeGraphReportsPanicKind(t *testing.T) {
	recorder := newRuntimeExceptionRecorder()
	graph := runtimegraph.MustRuntimeGraph[longEvent]("runtime-panic-kind").
		MustNode("A", runtimePanicHandler{}).
		MustEdge(topology.StartNode, "A").
		MustEdge("A", topology.EndNode)

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
	if err := d.Wait(); err == nil {
		t.Fatal("wait error is nil, want panic error")
	}
	got := recorder.wait(t)
	if got.Kind != disruptor.RuntimeGraphExceptionKindPanic {
		t.Fatalf("exception kind = %v, want panic", got.Kind)
	}
}

func TestRuntimeGraphWorkersExecuteIndependentNodesConcurrently(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	aStarted := make(chan struct{})
	bStarted := make(chan struct{})
	release := make(chan struct{})
	done := make(chan string, 2)
	graph := runtimegraph.MustRuntimeGraph[longEvent]("runtime-workers").
		MustNode("A", runtimeBlockingHandler{
			name:    "A",
			started: aStarted,
			peer:    bStarted,
			release: release,
			done:    done,
		}).
		MustNode("B", runtimeBlockingHandler{
			name:    "B",
			started: bStarted,
			peer:    aStarted,
			release: release,
			done:    done,
		}).
		MustNode("C", runtimeRecordingHandler{name: "C", handled: done}).
		MustEdge(topology.StartNode, "A").
		MustEdge(topology.StartNode, "B").
		MustEdge("A", "C").
		MustEdge("B", "C").
		MustEdge("C", topology.EndNode)

	d := newRuntimeGraphTestDisruptor(t, 8)
	processors, err := d.HandleRuntimeGraph(
		graph,
		disruptor.WithRuntimeGraphWorkers[longEvent](2),
	)
	if err != nil {
		t.Fatalf("handle runtime graph: %v", err)
	}
	if err := d.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer d.Stop()

	publishValues(t, d.RingBuffer(), 1)
	waitForRuntimeSignal(t, aStarted, "A started")
	waitForRuntimeSignal(t, bStarted, "B started")
	close(release)
	wantRuntimeHandlersAnyOrder(t, done, []string{"A", "B", "C"})
	waitForSequenceValue(t, processors.Sequence(), 0)
}

func TestRuntimeGraphRejectsWorkersAndExecutorTogether(t *testing.T) {
	graph := runtimegraph.MustRuntimeGraph[longEvent]("runtime-worker-conflict").
		MustNode("A", runtimeRecordingHandler{}).
		MustEdge(topology.StartNode, "A").
		MustEdge("A", topology.EndNode)

	d := newRuntimeGraphTestDisruptor(t, 8)
	_, err := d.HandleRuntimeGraph(
		graph,
		disruptor.WithRuntimeGraphWorkers[longEvent](2),
		disruptor.WithRuntimeGraphExecutor[longEvent](executor.NewInlineExecutor()),
	)
	if err == nil {
		t.Fatal("handle runtime graph error is nil, want conflict")
	}
}

func TestRuntimeGraphExternalExecutorIsCallerOwned(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	exec := &runtimeLifecycleExecutor{Executor: executor.NewInlineExecutor()}
	graph := runtimegraph.MustRuntimeGraph[longEvent]("runtime-external-executor").
		MustNode("A", runtimeRecordingHandler{}).
		MustEdge(topology.StartNode, "A").
		MustEdge("A", topology.EndNode)

	d := newRuntimeGraphTestDisruptor(t, 8)
	processors, err := d.HandleRuntimeGraph(
		graph,
		disruptor.WithRuntimeGraphExecutor[longEvent](exec),
	)
	if err != nil {
		t.Fatalf("handle runtime graph: %v", err)
	}
	if err := d.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	publishValues(t, d.RingBuffer(), 1)
	waitForSequenceValue(t, processors.Sequence(), 0)
	d.Stop()
	if err := d.Wait(); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if exec.shutdowns.Load() != 0 {
		t.Fatalf("external executor shutdowns = %d, want 0", exec.shutdowns.Load())
	}
}

func TestRuntimeGraphExecutorFailureUsesExceptionKindExecutor(t *testing.T) {
	submitErr := errors.New("executor submit failed")
	recorder := newRuntimeExceptionRecorder()
	graph := runtimegraph.MustRuntimeGraph[longEvent]("runtime-executor-failure").
		MustNode("A", runtimeRecordingHandler{}).
		MustEdge(topology.StartNode, "A").
		MustEdge("A", topology.EndNode)

	d := newRuntimeGraphTestDisruptor(t, 8)
	if _, err := d.HandleRuntimeGraph(
		graph,
		disruptor.WithRuntimeGraphExecutor[longEvent](runtimeFailingExecutor{err: submitErr}),
		disruptor.WithRuntimeGraphExceptionHandler[longEvent](recorder),
	); err != nil {
		t.Fatalf("handle runtime graph: %v", err)
	}
	if err := d.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	publishValues(t, d.RingBuffer(), 1)
	if err := d.Wait(); !errors.Is(err, submitErr) {
		t.Fatalf("wait error = %v, want submit error", err)
	}
	got := recorder.wait(t)
	if got.Kind != disruptor.RuntimeGraphExceptionKindExecutor {
		t.Fatalf("exception kind = %v, want executor", got.Kind)
	}
}

func TestRuntimeGraphParallelJoinWaitsForInFlightHandlersOnHalt(t *testing.T) {
	handlerErr := errors.New("halt after slow handler")
	finished := make(chan struct{})
	release := make(chan struct{})
	graph := runtimegraph.MustRuntimeGraph[longEvent]("runtime-halt-waits").
		MustNode("slow", runtimeReleaseHandler{
			release:  release,
			finished: finished,
		}).
		MustNode("fail", runtimeRecordingHandler{err: handlerErr}).
		MustEdge(topology.StartNode, "slow").
		MustEdge(topology.StartNode, "fail").
		MustEdge("slow", topology.EndNode).
		MustEdge("fail", topology.EndNode)

	d := newRuntimeGraphTestDisruptor(t, 8)
	if _, err := d.HandleRuntimeGraph(
		graph,
		disruptor.WithRuntimeGraphWorkers[longEvent](2),
	); err != nil {
		t.Fatalf("handle runtime graph: %v", err)
	}
	if err := d.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	publishValues(t, d.RingBuffer(), 1)
	waitForBlockedWait(t, d)
	close(release)
	if err := d.Wait(); !errors.Is(err, handlerErr) {
		t.Fatalf("wait error = %v, want handler error", err)
	}
	waitForRuntimeSignal(t, finished, "slow handler finished")
}

func TestRuntimeGraphExternalExecutorWaitsForInFlightHandlersOnHalt(t *testing.T) {
	handlerErr := errors.New("halt after external executor slow handler")
	finished := make(chan struct{})
	release := make(chan struct{})
	graph := runtimegraph.MustRuntimeGraph[longEvent]("runtime-external-halt-waits").
		MustNode("slow", runtimeReleaseHandler{
			release:  release,
			finished: finished,
		}).
		MustNode("fail", runtimeRecordingHandler{err: handlerErr}).
		MustEdge(topology.StartNode, "slow").
		MustEdge(topology.StartNode, "fail").
		MustEdge("slow", topology.EndNode).
		MustEdge("fail", topology.EndNode)

	pool, err := executor.NewFixedWorkerExecutor(
		executor.WithWorkers(2),
		executor.WithQueueSize(2),
	)
	if err != nil {
		t.Fatalf("new fixed worker executor: %v", err)
	}
	defer shutdownRuntimeExecutor(t, pool)

	d := newRuntimeGraphTestDisruptor(t, 8)
	if _, err := d.HandleRuntimeGraph(
		graph,
		disruptor.WithRuntimeGraphExecutor[longEvent](pool),
	); err != nil {
		t.Fatalf("handle runtime graph: %v", err)
	}
	if err := d.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	waitErr := make(chan error, 1)
	publishValues(t, d.RingBuffer(), 1)
	go runtimeGraphWait(d, waitErr)
	assertRuntimeWaitBlocked(t, waitErr)
	close(release)
	if err := <-waitErr; !errors.Is(err, handlerErr) {
		t.Fatalf("wait error = %v, want handler error", err)
	}
	waitForRuntimeSignal(t, finished, "external executor slow handler finished")
}

func newRuntimeGraphTestDisruptor(t *testing.T, size int) *disruptor.Disruptor[longEvent] {
	t.Helper()

	d, err := disruptor.New(
		event.FactoryFunc[longEvent](func() longEvent { return longEvent{} }),
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

func wantRuntimeHandlersAnyOrder(t *testing.T, handled <-chan string, want []string) {
	t.Helper()

	remaining := make(map[string]int, len(want))
	for _, name := range want {
		remaining[name]++
	}
	for range want {
		select {
		case got := <-handled:
			if remaining[got] == 0 {
				t.Fatalf("unexpected handler %q, remaining=%v", got, remaining)
			}
			remaining[got]--
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for handlers; remaining=%v", remaining)
		}
	}
}

func waitForRuntimeSignal(t *testing.T, signal <-chan struct{}, label string) {
	t.Helper()

	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", label)
	}
}

func waitForBlockedWait(t *testing.T, d *disruptor.Disruptor[longEvent]) {
	t.Helper()

	done := make(chan struct{})
	task := runtimeGraphBlockedWaitTask{
		disruptor: d,
		done:      done,
	}
	go task.run()

	select {
	case <-done:
		t.Fatal("wait returned before slow handler was released")
	case <-time.After(50 * time.Millisecond):
	}
}

type runtimeGraphBlockedWaitTask struct {
	disruptor *disruptor.Disruptor[longEvent]
	done      chan<- struct{}
}

func (task runtimeGraphBlockedWaitTask) run() {
	_ = task.disruptor.Wait()
	close(task.done)
}

func assertRuntimeWaitBlocked(t *testing.T, waitErr <-chan error) {
	t.Helper()

	select {
	case err := <-waitErr:
		t.Fatalf("wait returned before slow handler was released: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
}

func runtimeGraphWait(d *disruptor.Disruptor[longEvent], waitErr chan<- error) {
	waitErr <- d.Wait()
}

func shutdownRuntimeExecutor(t *testing.T, exec executor.Executor) {
	t.Helper()

	if err := exec.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown runtime executor: %v", err)
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

func (h runtimeRecordingHandler) OnEvent(request event.Request[longEvent]) error {
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
	request runtimegraph.EdgeConditionRequest[longEvent],
) (bool, error) {
	return bool(c), nil
}

type runtimeErrorCondition struct {
	err error
}

func (c runtimeErrorCondition) Evaluate(
	request runtimegraph.EdgeConditionRequest[longEvent],
) (bool, error) {
	return false, c.err
}

type runtimeFailOnceHandler struct {
	err  error
	once sync.Once
}

func (h *runtimeFailOnceHandler) OnEvent(request event.Request[longEvent]) error {
	var err error
	h.once.Do(func() {
		err = h.err
	})

	return err
}

type runtimePanicHandler struct{}

func (runtimePanicHandler) OnEvent(request event.Request[longEvent]) error {
	panic("runtime graph panic")
}

type runtimeBlockingHandler struct {
	name    string
	started chan<- struct{}
	peer    <-chan struct{}
	release <-chan struct{}
	done    chan<- string
}

func (h runtimeBlockingHandler) OnEvent(request event.Request[longEvent]) error {
	close(h.started)
	<-h.peer
	<-h.release
	h.done <- h.name

	return nil
}

type runtimeReleaseHandler struct {
	release  <-chan struct{}
	finished chan<- struct{}
}

func (h runtimeReleaseHandler) OnEvent(request event.Request[longEvent]) error {
	<-h.release
	close(h.finished)

	return nil
}

type runtimeLifecycleExecutor struct {
	executor.Executor
	shutdowns atomic.Int64
}

func (e *runtimeLifecycleExecutor) Shutdown(ctx context.Context) error {
	e.shutdowns.Add(1)

	return e.Executor.Shutdown(ctx)
}

type runtimeFailingExecutor struct {
	err error
}

func (e runtimeFailingExecutor) Submit(executor.SubmitRequest) error {
	return e.err
}

func (e runtimeFailingExecutor) Shutdown(context.Context) error {
	return nil
}

type runtimeDecimalRaw struct {
	cents int64
}

type runtimeDecimalNumber struct {
	cents int64
}

func (n runtimeDecimalNumber) NumberKind() expression.NumberKind {
	return "runtime.decimal"
}

type runtimeDecimalAdapter struct {
	err error
}

func (a runtimeDecimalAdapter) Convert(
	request expression.ValueConvertRequest,
) (expression.Value, bool, error) {
	value, ok := request.Value.(runtimeDecimalRaw)
	if !ok {
		return expression.Value{}, false, nil
	}

	return expression.Value{
		Kind:   expression.ValueNumber,
		Number: runtimeDecimalNumber(value),
	}, true, nil
}

func (a runtimeDecimalAdapter) CompareNumber(
	request expression.NumberCompareRequest,
) (int, bool, error) {
	if _, ok := request.Left.Number.(runtimeDecimalNumber); !ok {
		return 0, false, nil
	}

	return 0, true, a.err
}

func (a runtimeDecimalAdapter) ConvertNumberToBool(
	request expression.NumberBoolRequest,
) (bool, bool, error) {
	return false, false, nil
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
) event.ExceptionAction {
	r.requests <- request

	return event.ExceptionActionHalt
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

func (r runtimeExceptionRecorder) assertNoRequest(t *testing.T) {
	t.Helper()

	select {
	case request := <-r.requests:
		t.Fatalf("unexpected runtime graph exception: %#v", request)
	case <-time.After(50 * time.Millisecond):
	}
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
