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

package runtimegraph

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/photowey/disruptor.go/pkg/event"
	"github.com/photowey/disruptor.go/pkg/expression"
	"github.com/photowey/disruptor.go/pkg/graph"
	"github.com/photowey/disruptor.go/pkg/runtimevars"
)

type runtimeGraphTestEvent struct {
	Value int64
}

type runtimeGraphNoopHandler struct{}

func (runtimeGraphNoopHandler) OnEvent(request event.Request[runtimeGraphTestEvent]) error {
	return nil
}

type runtimeGraphCondition bool

func (c runtimeGraphCondition) Evaluate(
	request EdgeConditionRequest[runtimeGraphTestEvent],
) (bool, error) {
	return bool(c), nil
}

func TestNewRuntimeGraphValidatesNameAndMustPanics(t *testing.T) {
	if _, err := NewRuntimeGraph[runtimeGraphTestEvent](" "); !errors.Is(err, ErrInvalid) {
		t.Fatalf("empty name error = %v, want runtimegraph.ErrInvalid", err)
	}
	if _, err := NewRuntimeGraph[runtimeGraphTestEvent]("bad\nname"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("control character name error = %v, want runtimegraph.ErrInvalid", err)
	}

	runtimeGraph, err := NewRuntimeGraph[runtimeGraphTestEvent](" orders ")
	if err != nil {
		t.Fatalf("new runtime graph: %v", err)
	}
	if got := runtimeGraph.Name(); got != "orders" {
		t.Fatalf("runtime graph name = %q, want orders", got)
	}

	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatal("expected MustRuntimeGraph to panic")
		}
	}()
	_ = MustRuntimeGraph[runtimeGraphTestEvent]("")
}

func TestRuntimeGraphNodeOptionsAndSnapshot(t *testing.T) {
	handler := runtimeGraphNoopHandler{}
	runtimeGraph := MustRuntimeGraph[runtimeGraphTestEvent]("route").
		MustNode(
			" decide ",
			handler,
			WithNodeLabel[runtimeGraphTestEvent]("Decide"),
			WithNodeMetadata[runtimeGraphTestEvent](" role ", " router "),
			WithNodeExceptionHandler[runtimeGraphTestEvent](event.NewIgnoreExceptionHandler[runtimeGraphTestEvent]()),
		).
		MustNode("fast", handler).
		MustEdge(graph.StartNode, "decide").
		MustEdge("decide", "fast", WhenExpression[runtimeGraphTestEvent](`${route.fast}`)).
		MustEdge("fast", graph.EndNode)

	snapshot := runtimeGraph.Snapshot()
	if got := snapshot.Nodes[1].Name; got != "decide" {
		t.Fatalf("node name = %q, want decide", got)
	}
	if got := snapshot.Nodes[1].Label; got != "Decide" {
		t.Fatalf("node label = %q, want Decide", got)
	}
	if got := snapshot.Nodes[1].Metadata["role"]; got != "router" {
		t.Fatalf("node metadata role = %q, want router", got)
	}
	if got := snapshot.Edges[1].Condition; got != `${route.fast}` {
		t.Fatalf("edge condition = %q, want expression label", got)
	}
	if got := snapshot.Entries; len(got) != 1 || got[0] != "decide" {
		t.Fatalf("entries = %v, want [decide]", got)
	}
	if got := snapshot.Exits; len(got) != 1 || got[0] != "fast" {
		t.Fatalf("exits = %v, want [fast]", got)
	}
}

func TestRuntimeGraphRejectsInvalidNodesAndEdges(t *testing.T) {
	runtimeGraph := MustRuntimeGraph[runtimeGraphTestEvent]("invalid")
	handler := runtimeGraphNoopHandler{}

	if err := runtimeGraph.Node(graph.StartNode, handler); !errors.Is(err, ErrInvalid) {
		t.Fatalf("reserved start node error = %v, want runtimegraph.ErrInvalid", err)
	}
	if err := runtimeGraph.Node("missing-handler", nil); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil handler error = %v, want runtimegraph.ErrInvalid", err)
	}
	runtimeGraph.MustNode("A", handler)
	if err := runtimeGraph.Node("A", handler); !errors.Is(err, ErrInvalid) {
		t.Fatalf("duplicate node error = %v, want runtimegraph.ErrInvalid", err)
	}
	if err := runtimeGraph.Edge(graph.StartNode, graph.EndNode); !errors.Is(err, ErrInvalid) {
		t.Fatalf("start to end error = %v, want runtimegraph.ErrInvalid", err)
	}
	if err := runtimeGraph.Edge("A", graph.StartNode); !errors.Is(err, ErrInvalid) {
		t.Fatalf("real to start error = %v, want runtimegraph.ErrInvalid", err)
	}
	if err := runtimeGraph.Edge("A", "A"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("self edge error = %v, want runtimegraph.ErrInvalid", err)
	}
	if err := runtimeGraph.Edge("A", "missing"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unknown target error = %v, want runtimegraph.ErrInvalid", err)
	}
}

func TestRuntimeGraphBuildPlanEvaluatesConditionsAndFreezes(t *testing.T) {
	runtimeGraph := MustRuntimeGraph[runtimeGraphTestEvent]("plan").
		MustNode("route", runtimeGraphNoopHandler{}).
		MustNode("fast", runtimeGraphNoopHandler{}).
		MustNode("audit", runtimeGraphNoopHandler{}).
		MustEdge(graph.StartNode, "route").
		MustEdge("route", "fast", WhenExpression[runtimeGraphTestEvent](`${route.fast}`)).
		MustEdge("route", "audit", WhenCondition[runtimeGraphTestEvent](runtimeGraphCondition(false))).
		MustEdge("fast", graph.EndNode).
		MustEdge("audit", graph.EndNode)

	plan, err := runtimeGraph.BuildPlan()
	if err != nil {
		t.Fatalf("build plan: %v", err)
	}
	if got := len(plan.Start); got != 1 {
		t.Fatalf("start edge count = %d, want 1", got)
	}
	if got := plan.Nodes["route"].Incoming; got != 1 {
		t.Fatalf("route incoming = %d, want 1", got)
	}

	runtimeContext := runtimevars.NewContext[runtimeGraphTestEvent](
		runtimevars.Request[runtimeGraphTestEvent]{Context: context.Background()},
		plan.Name,
		nil,
		nil,
	)
	if err := runtimeContext.Set("route.fast", true); err != nil {
		t.Fatalf("set runtime variable: %v", err)
	}

	var fastEdge PlanEdge[runtimeGraphTestEvent]
	var auditEdge PlanEdge[runtimeGraphTestEvent]
	for _, edge := range plan.Nodes["route"].Outgoing {
		switch edge.To {
		case "fast":
			fastEdge = edge
		case "audit":
			auditEdge = edge
		}
	}
	fast, err := fastEdge.Evaluate(EdgeConditionRequest[runtimeGraphTestEvent]{
		Context: context.Background(),
		Runtime: runtimeContext,
	})
	if err != nil {
		t.Fatalf("evaluate fast edge: %v", err)
	}
	if !fast {
		t.Fatal("fast edge = false, want true")
	}
	audit, err := auditEdge.Evaluate(EdgeConditionRequest[runtimeGraphTestEvent]{
		Context: context.Background(),
		Runtime: runtimeContext,
	})
	if err != nil {
		t.Fatalf("evaluate audit edge: %v", err)
	}
	if audit {
		t.Fatal("audit edge = true, want false")
	}

	if err := runtimeGraph.Edge("route", "audit"); !errors.Is(err, ErrFrozen) {
		t.Fatalf("edge after build plan error = %v, want runtimegraph.ErrFrozen", err)
	}
	if _, err := runtimeGraph.BuildPlan(); !errors.Is(err, ErrHandled) {
		t.Fatalf("second build plan error = %v, want runtimegraph.ErrHandled", err)
	}
}

func TestRuntimeGraphConditionAndCompilerErrors(t *testing.T) {
	var nilCondition EdgeConditionFunc[runtimeGraphTestEvent]
	if _, err := nilCondition.Evaluate(EdgeConditionRequest[runtimeGraphTestEvent]{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil condition error = %v, want runtimegraph.ErrInvalid", err)
	}

	if _, err := NewRuntimeGraph[runtimeGraphTestEvent](
		"bad-compiler",
		WithExpressionCompiler(nil),
	); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil compiler error = %v, want runtimegraph.ErrInvalid", err)
	}

	compileErr := errors.New("compile failed")
	runtimeGraph := MustRuntimeGraph[runtimeGraphTestEvent](
		"compile-error",
		WithExpressionCompiler(errorExpressionCompiler{err: compileErr}),
	).MustNode("A", runtimeGraphNoopHandler{}).
		MustNode("B", runtimeGraphNoopHandler{})

	err := runtimeGraph.Edge("A", "B", WhenExpression[runtimeGraphTestEvent](`${ok}`))
	if !errors.Is(err, compileErr) {
		t.Fatalf("compile error = %v, want wrapped compile error", err)
	}
}

func TestRuntimeGraphExportFormatsIncludeConditions(t *testing.T) {
	runtimeGraph := MustRuntimeGraph[runtimeGraphTestEvent]("export").
		MustNode("A", runtimeGraphNoopHandler{}).
		MustNode("B", runtimeGraphNoopHandler{}).
		MustEdge(graph.StartNode, "A").
		MustEdge("A", "B", WhenExpression[runtimeGraphTestEvent](`${enabled}`)).
		MustEdge("B", graph.EndNode)

	mermaid := runtimeGraph.Mermaid()
	if !strings.Contains(mermaid, "|${enabled}|") {
		t.Fatalf("mermaid = %q, want expression condition label", mermaid)
	}
	dot := runtimeGraph.DOT()
	if !strings.Contains(dot, "label=\"${enabled}\"") {
		t.Fatalf("dot = %q, want expression condition label", dot)
	}
}

type errorExpressionCompiler struct {
	err error
}

func (c errorExpressionCompiler) Compile(expression.Expression) (expression.BoolExpression, error) {
	return nil, c.err
}
