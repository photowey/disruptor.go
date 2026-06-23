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
	"errors"
	"strings"
	"testing"

	"github.com/photowey/disruptor.go/pkg/disruptor"
)

func TestNewGraphValidatesName(t *testing.T) {
	if _, err := disruptor.NewGraph[longEvent](" "); !errors.Is(err, disruptor.ErrInvalidGraph) {
		t.Fatalf("empty name error = %v, want ErrInvalidGraph", err)
	}
	if _, err := disruptor.NewGraph[longEvent]("bad\nname"); !errors.Is(err, disruptor.ErrInvalidGraph) {
		t.Fatalf("control character name error = %v, want ErrInvalidGraph", err)
	}

	graph, err := disruptor.NewGraph[longEvent](" orders ")
	if err != nil {
		t.Fatalf("new graph: %v", err)
	}
	if got := graph.Name(); got != "orders" {
		t.Fatalf("graph name = %q, want orders", got)
	}

	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatal("expected MustGraph to panic")
		}
	}()
	_ = disruptor.MustGraph[longEvent]("")
}

func TestGraphNodeValidatesNameAndHandler(t *testing.T) {
	graph := mustTestGraph(t, "orders")
	handler := graphNoopHandler{}

	if err := graph.Node(" validate ", handler); err != nil {
		t.Fatalf("node: %v", err)
	}
	if err := graph.Node("validate", handler); !errors.Is(err, disruptor.ErrInvalidGraph) {
		t.Fatalf("duplicate node error = %v, want ErrInvalidGraph", err)
	}
	if err := graph.Node("bad\nnode", handler); !errors.Is(err, disruptor.ErrInvalidGraph) {
		t.Fatalf("control character node error = %v, want ErrInvalidGraph", err)
	}
	if err := graph.Node("missing-handler", nil); !errors.Is(err, disruptor.ErrInvalidGraph) {
		t.Fatalf("nil handler error = %v, want ErrInvalidGraph", err)
	}

	snapshot := graph.Snapshot()
	if len(snapshot.Nodes) != 1 {
		t.Fatalf("node count = %d, want 1", len(snapshot.Nodes))
	}
	if got := snapshot.Nodes[0].Name; got != "validate" {
		t.Fatalf("node name = %q, want validate", got)
	}
}

func TestGraphEdgeValidatesEndpoints(t *testing.T) {
	graph := mustTestGraph(t, "orders")
	graph.MustNode("A", graphNoopHandler{}).
		MustNode("B", graphNoopHandler{})

	if err := graph.Edge("A", "B"); err != nil {
		t.Fatalf("edge: %v", err)
	}
	if err := graph.Edge("A", "B"); err != nil {
		t.Fatalf("duplicate edge should be idempotent: %v", err)
	}
	if err := graph.Edge("A", "A"); !errors.Is(err, disruptor.ErrInvalidGraph) {
		t.Fatalf("self edge error = %v, want ErrInvalidGraph", err)
	}
	if err := graph.Edge("A", "missing"); !errors.Is(err, disruptor.ErrInvalidGraph) {
		t.Fatalf("unknown target error = %v, want ErrInvalidGraph", err)
	}

	snapshot := graph.Snapshot()
	if len(snapshot.Edges) != 1 {
		t.Fatalf("edge count = %d, want 1", len(snapshot.Edges))
	}
	if snapshot.Edges[0].From != "A" || snapshot.Edges[0].To != "B" {
		t.Fatalf("edge = %+v, want A -> B", snapshot.Edges[0])
	}
}

func TestGraphJoinExpandsEdges(t *testing.T) {
	graph := mustTestGraph(t, "orders")
	graph.MustNode("A", graphNoopHandler{}).
		MustNode("B", graphNoopHandler{}).
		MustNode("C", graphNoopHandler{}).
		MustNode("D", graphNoopHandler{})

	if err := graph.Join("A", "B").To("C", "D"); err != nil {
		t.Fatalf("join to: %v", err)
	}
	if err := graph.Join().To("C"); !errors.Is(err, disruptor.ErrInvalidGraph) {
		t.Fatalf("empty join sources error = %v, want ErrInvalidGraph", err)
	}
	if err := graph.Join("A").To(); !errors.Is(err, disruptor.ErrInvalidGraph) {
		t.Fatalf("empty join targets error = %v, want ErrInvalidGraph", err)
	}

	got := graph.Snapshot().Edges
	want := []disruptor.GraphEdgeSnapshot{
		{From: "A", To: "C"},
		{From: "A", To: "D"},
		{From: "B", To: "C"},
		{From: "B", To: "D"},
	}
	if len(got) != len(want) {
		t.Fatalf("edge count = %d, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("edge[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestGraphValidateRejectsCycles(t *testing.T) {
	graph := mustTestGraph(t, "cycle")
	graph.MustNode("A", graphNoopHandler{}).
		MustNode("B", graphNoopHandler{}).
		MustNode("C", graphNoopHandler{}).
		MustEdge("A", "B").
		MustEdge("B", "C").
		MustEdge("C", "A")

	err := graph.Validate()
	if !errors.Is(err, disruptor.ErrInvalidGraph) {
		t.Fatalf("validate error = %v, want ErrInvalidGraph", err)
	}
	if !strings.Contains(err.Error(), "A -> B -> C -> A") {
		t.Fatalf("cycle error = %q, want cycle path", err)
	}
}

func TestGraphValidateRejectsIsolatedNodeInMultiNodeGraph(t *testing.T) {
	graph := mustTestGraph(t, "isolated")
	graph.MustNode("A", graphNoopHandler{}).
		MustNode("B", graphNoopHandler{}).
		MustNode("C", graphNoopHandler{}).
		MustEdge("A", "B")

	err := graph.Validate()
	if !errors.Is(err, disruptor.ErrInvalidGraph) {
		t.Fatalf("validate error = %v, want ErrInvalidGraph", err)
	}
	if !strings.Contains(err.Error(), "node C is isolated") {
		t.Fatalf("isolated error = %q, want node name", err)
	}

	single := mustTestGraph(t, "single")
	single.MustNode("A", graphNoopHandler{})
	if err := single.Validate(); err != nil {
		t.Fatalf("single-node graph validate: %v", err)
	}
}

func TestGraphSnapshotIsDeterministicAndDefensive(t *testing.T) {
	graph := mustTestGraph(t, "orders")
	graph.MustNode("B", graphNoopHandler{}, disruptor.WithNodeMetadata[longEvent]("role", "middle")).
		MustNode("A", graphNoopHandler{}).
		MustNode("C", graphNoopHandler{}).
		MustEdge("A", "B").
		MustEdge("B", "C")

	snapshot := graph.Snapshot()
	if got := snapshot.Sources; len(got) != 1 || got[0] != "A" {
		t.Fatalf("sources = %v, want [A]", got)
	}
	if got := snapshot.Leaves; len(got) != 1 || got[0] != "C" {
		t.Fatalf("leaves = %v, want [C]", got)
	}
	if got := snapshot.Nodes[0].Name; got != "A" {
		t.Fatalf("first node = %q, want A", got)
	}

	snapshot.Nodes[1].Metadata["role"] = "changed"
	fresh := graph.Snapshot()
	if got := fresh.Nodes[1].Metadata["role"]; got != "middle" {
		t.Fatalf("metadata after external mutation = %q, want middle", got)
	}
}

func TestGraphMermaidAndDOTUseGeneratedIDs(t *testing.T) {
	graph := mustTestGraph(t, `orders"graph`)
	graph.MustNode(`A"[`, graphNoopHandler{}).
		MustNode(`B\]`, graphNoopHandler{}).
		MustEdge(`A"[`, `B\]`)

	mermaid := graph.Mermaid()
	if !strings.Contains(mermaid, "n0") || !strings.Contains(mermaid, "n1") {
		t.Fatalf("mermaid = %q, want generated node ids", mermaid)
	}
	if strings.Contains(mermaid, `A"[ --> B\]`) {
		t.Fatalf("mermaid = %q, should not use raw names as syntax ids", mermaid)
	}

	dot := graph.DOT()
	if !strings.Contains(dot, "n0") || !strings.Contains(dot, "n1") {
		t.Fatalf("dot = %q, want generated node ids", dot)
	}
	if strings.Contains(dot, `A"[ -> B\]`) {
		t.Fatalf("dot = %q, should not use raw names as syntax ids", dot)
	}
}

func mustTestGraph(t *testing.T, name string) *disruptor.Graph[longEvent] {
	t.Helper()

	graph, err := disruptor.NewGraph[longEvent](name)
	if err != nil {
		t.Fatalf("new graph: %v", err)
	}

	return graph
}

type graphNoopHandler struct{}

func (graphNoopHandler) OnEvent(request disruptor.EventRequest[longEvent]) error {
	return nil
}
