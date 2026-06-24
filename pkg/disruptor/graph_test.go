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
	if len(snapshot.Nodes) != 3 {
		t.Fatalf("node count = %d, want 3", len(snapshot.Nodes))
	}
	if got := snapshot.Nodes[1].Name; got != "validate" {
		t.Fatalf("node name = %q, want validate", got)
	}
}

func TestGraphNodeRejectsReservedVirtualNames(t *testing.T) {
	graph := mustTestGraph(t, "orders")
	handler := graphNoopHandler{}

	if err := graph.Node(disruptor.GraphStartNode, handler); !errors.Is(err, disruptor.ErrInvalidGraph) {
		t.Fatalf("start node error = %v, want ErrInvalidGraph", err)
	}
	if err := graph.Node(disruptor.GraphEndNode, handler); !errors.Is(err, disruptor.ErrInvalidGraph) {
		t.Fatalf("end node error = %v, want ErrInvalidGraph", err)
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
	if got := realGraphEdges(snapshot.Edges); len(got) != 1 {
		t.Fatalf("real edge count = %d, want 1", len(got))
	}
	if got := realGraphEdges(snapshot.Edges)[0]; got.From != "A" || got.To != "B" {
		t.Fatalf("edge = %+v, want A -> B", got)
	}
}

func TestGraphEdgeAllowsExplicitTerminalEdges(t *testing.T) {
	graph := mustTestGraph(t, "orders")
	graph.MustNode("A", graphNoopHandler{}).
		MustNode("B", graphNoopHandler{}).
		MustEdge(disruptor.GraphStartNode, "A").
		MustEdge("A", "B").
		MustEdge("B", disruptor.GraphEndNode)

	snapshot := graph.Snapshot()
	wantNodes := []string{disruptor.GraphStartNode, "A", "B", disruptor.GraphEndNode}
	if len(snapshot.Nodes) != len(wantNodes) {
		t.Fatalf("node count = %d, want %d: %+v", len(snapshot.Nodes), len(wantNodes), snapshot.Nodes)
	}
	for i, want := range wantNodes {
		if snapshot.Nodes[i].Name != want {
			t.Fatalf("node[%d] = %+v, want %q", i, snapshot.Nodes[i], want)
		}
	}

	wantEdges := []disruptor.GraphEdgeSnapshot{
		{From: disruptor.GraphStartNode, To: "A"},
		{From: "A", To: "B"},
		{From: "B", To: disruptor.GraphEndNode},
	}
	if len(snapshot.Edges) != len(wantEdges) {
		t.Fatalf("edge count = %d, want %d: %+v", len(snapshot.Edges), len(wantEdges), snapshot.Edges)
	}
	for i, want := range wantEdges {
		if snapshot.Edges[i] != want {
			t.Fatalf("edge[%d] = %+v, want %+v", i, snapshot.Edges[i], want)
		}
	}
	if got := snapshot.Entries; len(got) != 1 || got[0] != "A" {
		t.Fatalf("entries = %v, want [A]", got)
	}
	if got := snapshot.Exits; len(got) != 1 || got[0] != "B" {
		t.Fatalf("exits = %v, want [B]", got)
	}
}

func TestGraphEdgeRejectsInvalidTerminalEdges(t *testing.T) {
	graph := mustTestGraph(t, "orders")
	graph.MustNode("A", graphNoopHandler{})

	cases := []struct {
		name string
		from string
		to   string
	}{
		{name: "start to end", from: disruptor.GraphStartNode, to: disruptor.GraphEndNode},
		{name: "start to start", from: disruptor.GraphStartNode, to: disruptor.GraphStartNode},
		{name: "end to end", from: disruptor.GraphEndNode, to: disruptor.GraphEndNode},
		{name: "real to start", from: "A", to: disruptor.GraphStartNode},
		{name: "end to real", from: disruptor.GraphEndNode, to: "A"},
		{name: "end to start", from: disruptor.GraphEndNode, to: disruptor.GraphStartNode},
		{name: "start to missing", from: disruptor.GraphStartNode, to: "missing"},
		{name: "missing to end", from: "missing", to: disruptor.GraphEndNode},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := graph.Edge(tc.from, tc.to)
			if !errors.Is(err, disruptor.ErrInvalidGraph) {
				t.Fatalf("edge error = %v, want ErrInvalidGraph", err)
			}
		})
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

	got := realGraphEdges(graph.Snapshot().Edges)
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

func TestGraphValidateRejectsMissingExplicitTerminals(t *testing.T) {
	graph := mustTestGraph(t, "missing-terminals")
	graph.MustNode("A", graphNoopHandler{}).
		MustNode("B", graphNoopHandler{}).
		MustEdge("A", "B")

	err := graph.Validate()
	if !errors.Is(err, disruptor.ErrInvalidGraph) {
		t.Fatalf("validate error = %v, want ErrInvalidGraph", err)
	}
	if !strings.Contains(err.Error(), "entry") {
		t.Fatalf("validate error = %q, want entry message", err)
	}
}

func TestGraphValidateRejectsEntrySourceMismatch(t *testing.T) {
	graph := mustTestGraph(t, "entry-mismatch")
	graph.MustNode("A", graphNoopHandler{}).
		MustNode("B", graphNoopHandler{}).
		MustEdge(disruptor.GraphStartNode, "B").
		MustEdge("A", "B").
		MustEdge("B", disruptor.GraphEndNode)

	err := graph.Validate()
	if !errors.Is(err, disruptor.ErrInvalidGraph) {
		t.Fatalf("validate error = %v, want ErrInvalidGraph", err)
	}
	if !strings.Contains(err.Error(), "entries must match sources") {
		t.Fatalf("validate error = %q, want entry/source mismatch", err)
	}
}

func TestGraphValidateRejectsExitLeafMismatch(t *testing.T) {
	graph := mustTestGraph(t, "exit-mismatch")
	graph.MustNode("A", graphNoopHandler{}).
		MustNode("B", graphNoopHandler{}).
		MustEdge(disruptor.GraphStartNode, "A").
		MustEdge("A", "B").
		MustEdge("A", disruptor.GraphEndNode)

	err := graph.Validate()
	if !errors.Is(err, disruptor.ErrInvalidGraph) {
		t.Fatalf("validate error = %v, want ErrInvalidGraph", err)
	}
	if !strings.Contains(err.Error(), "exits must match leaves") {
		t.Fatalf("validate error = %q, want exit/leaf mismatch", err)
	}
}

func TestGraphValidateAcceptsSingleExplicitTerminalNode(t *testing.T) {
	single := mustTestGraph(t, "single")
	single.MustNode("A", graphNoopHandler{}).
		MustEdge(disruptor.GraphStartNode, "A").
		MustEdge("A", disruptor.GraphEndNode)
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
	if got := snapshot.Nodes[1].Name; got != "A" {
		t.Fatalf("first real node = %q, want A", got)
	}

	snapshot.Nodes[2].Metadata["role"] = "changed"
	fresh := graph.Snapshot()
	if got := fresh.Nodes[2].Metadata["role"]; got != "middle" {
		t.Fatalf("metadata after external mutation = %q, want middle", got)
	}
}

func TestGraphSnapshotIncludesVirtualTerminals(t *testing.T) {
	graph := mustTestGraph(t, "orders")
	graph.MustNode("A", graphNoopHandler{}).
		MustNode("B", graphNoopHandler{}).
		MustEdge(disruptor.GraphStartNode, "A").
		MustEdge("A", "B").
		MustEdge("B", disruptor.GraphEndNode)

	snapshot := graph.Snapshot()
	wantNodes := []string{disruptor.GraphStartNode, "A", "B", disruptor.GraphEndNode}
	if len(snapshot.Nodes) != len(wantNodes) {
		t.Fatalf("node count = %d, want %d: %+v", len(snapshot.Nodes), len(wantNodes), snapshot.Nodes)
	}
	for i, want := range wantNodes {
		if snapshot.Nodes[i].Name != want {
			t.Fatalf("node[%d] = %+v, want %q", i, snapshot.Nodes[i], want)
		}
	}

	wantEdges := []disruptor.GraphEdgeSnapshot{
		{From: disruptor.GraphStartNode, To: "A"},
		{From: "A", To: "B"},
		{From: "B", To: disruptor.GraphEndNode},
	}
	if len(snapshot.Edges) != len(wantEdges) {
		t.Fatalf("edge count = %d, want %d: %+v", len(snapshot.Edges), len(wantEdges), snapshot.Edges)
	}
	for i, want := range wantEdges {
		if snapshot.Edges[i] != want {
			t.Fatalf("edge[%d] = %+v, want %+v", i, snapshot.Edges[i], want)
		}
	}
	if got := snapshot.Sources; len(got) != 1 || got[0] != "A" {
		t.Fatalf("sources = %v, want real source [A]", got)
	}
	if got := snapshot.Leaves; len(got) != 1 || got[0] != "B" {
		t.Fatalf("leaves = %v, want real leaf [B]", got)
	}
	if got := snapshot.Entries; len(got) != 1 || got[0] != "A" {
		t.Fatalf("entries = %v, want [A]", got)
	}
	if got := snapshot.Exits; len(got) != 1 || got[0] != "B" {
		t.Fatalf("exits = %v, want [B]", got)
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

func TestGraphMermaidAndDOTIncludeVirtualTerminals(t *testing.T) {
	graph := mustTestGraph(t, "orders")
	graph.MustNode("A", graphNoopHandler{})

	mermaid := graph.Mermaid()
	if !strings.Contains(mermaid, `["START"]`) || !strings.Contains(mermaid, `["END"]`) {
		t.Fatalf("mermaid = %q, want START and END virtual nodes", mermaid)
	}

	dot := graph.DOT()
	if !strings.Contains(dot, `label="START"`) || !strings.Contains(dot, `label="END"`) {
		t.Fatalf("dot = %q, want START and END virtual nodes", dot)
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

func realGraphEdges(edges []disruptor.GraphEdgeSnapshot) []disruptor.GraphEdgeSnapshot {
	real := make([]disruptor.GraphEdgeSnapshot, 0, len(edges))
	for _, edge := range edges {
		if edge.From == disruptor.GraphStartNode || edge.To == disruptor.GraphEndNode {
			continue
		}
		real = append(real, edge)
	}

	return real
}

type graphNoopHandler struct{}

func (graphNoopHandler) OnEvent(request disruptor.EventRequest[longEvent]) error {
	return nil
}
