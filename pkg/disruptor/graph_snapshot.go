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

package disruptor

import (
	"sort"
	"strconv"
	"strings"
)

// GraphSnapshot is a handler-free view of a graph.
type GraphSnapshot struct {
	Name    string
	Frozen  bool
	Nodes   []GraphNodeSnapshot
	Edges   []GraphEdgeSnapshot
	Sources []string
	Leaves  []string
}

// GraphNodeSnapshot describes one graph node.
type GraphNodeSnapshot struct {
	Name     string
	Label    string
	Metadata map[string]string
}

// GraphEdgeSnapshot describes one graph dependency edge.
type GraphEdgeSnapshot struct {
	From string
	To   string
}

// Snapshot returns a deterministic, defensive graph snapshot.
func (g *Graph[T]) Snapshot() GraphSnapshot {
	if g == nil {
		return GraphSnapshot{}
	}

	g.mu.RLock()
	defer g.mu.RUnlock()

	return g.snapshotLocked()
}

func (g *Graph[T]) snapshotLocked() GraphSnapshot {
	nodes := make([]GraphNodeSnapshot, 0, len(g.nodes))
	for _, node := range g.nodes {
		nodes = append(nodes, GraphNodeSnapshot{
			Name:     node.name,
			Label:    node.label,
			Metadata: copyStringMap(node.metadata),
		})
	}
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].Name < nodes[j].Name
	})

	edges := make([]GraphEdgeSnapshot, 0, len(g.edges))
	for edge := range g.edges {
		edges = append(edges, edge)
	}
	sortEdges(edges)

	sources, leaves := graphTerminals(nodes, edges)

	return GraphSnapshot{
		Name:    g.name,
		Frozen:  g.frozen,
		Nodes:   nodes,
		Edges:   edges,
		Sources: sources,
		Leaves:  leaves,
	}
}

// Mermaid renders the graph as a Mermaid flowchart.
func (g *Graph[T]) Mermaid() string {
	snapshot := g.Snapshot()
	ids := graphNodeIDs(snapshot.Nodes)

	var builder strings.Builder
	builder.WriteString("flowchart LR\n")
	for i, node := range snapshot.Nodes {
		builder.WriteString("    ")
		builder.WriteString(ids[node.Name])
		builder.WriteString("[\"")
		builder.WriteString(escapeMermaidLabel(nodeLabel(node)))
		builder.WriteString("\"]\n")
		if i == len(snapshot.Nodes)-1 && len(snapshot.Edges) == 0 {
			continue
		}
	}
	for _, edge := range snapshot.Edges {
		builder.WriteString("    ")
		builder.WriteString(ids[edge.From])
		builder.WriteString(" --> ")
		builder.WriteString(ids[edge.To])
		builder.WriteString("\n")
	}

	return builder.String()
}

// DOT renders the graph as a Graphviz DOT graph.
func (g *Graph[T]) DOT() string {
	snapshot := g.Snapshot()
	ids := graphNodeIDs(snapshot.Nodes)

	var builder strings.Builder
	builder.WriteString("digraph ")
	builder.WriteString(strconv.Quote(snapshot.Name))
	builder.WriteString(" {\n")
	for _, node := range snapshot.Nodes {
		builder.WriteString("    ")
		builder.WriteString(ids[node.Name])
		builder.WriteString(" [label=")
		builder.WriteString(strconv.Quote(nodeLabel(node)))
		builder.WriteString("];\n")
	}
	for _, edge := range snapshot.Edges {
		builder.WriteString("    ")
		builder.WriteString(ids[edge.From])
		builder.WriteString(" -> ")
		builder.WriteString(ids[edge.To])
		builder.WriteString(";\n")
	}
	builder.WriteString("}\n")

	return builder.String()
}

func (g *Graph[T]) findCycleLocked() []string {
	snapshot := g.snapshotLocked()
	adjacency := make(map[string][]string, len(snapshot.Nodes))
	for _, edge := range snapshot.Edges {
		adjacency[edge.From] = append(adjacency[edge.From], edge.To)
	}
	for node := range adjacency {
		sort.Strings(adjacency[node])
	}

	state := make(map[string]uint8, len(snapshot.Nodes))
	stack := make([]string, 0, len(snapshot.Nodes))

	var visit func(name string) []string
	visit = func(name string) []string {
		state[name] = 1
		stack = append(stack, name)
		for _, next := range adjacency[name] {
			switch state[next] {
			case 0:
				if cycle := visit(next); len(cycle) > 0 {
					return cycle
				}
			case 1:
				for i, stacked := range stack {
					if stacked == next {
						return append(append([]string(nil), stack[i:]...), next)
					}
				}
			}
		}
		stack = stack[:len(stack)-1]
		state[name] = 2
		return nil
	}

	for _, node := range snapshot.Nodes {
		if state[node.Name] != 0 {
			continue
		}
		if cycle := visit(node.Name); len(cycle) > 0 {
			return cycle
		}
	}

	return nil
}

func graphTerminals(
	nodes []GraphNodeSnapshot,
	edges []GraphEdgeSnapshot,
) ([]string, []string) {
	incoming := make(map[string]int, len(nodes))
	outgoing := make(map[string]int, len(nodes))
	for _, node := range nodes {
		incoming[node.Name] = 0
		outgoing[node.Name] = 0
	}
	for _, edge := range edges {
		outgoing[edge.From]++
		incoming[edge.To]++
	}

	sources := make([]string, 0, len(nodes))
	leaves := make([]string, 0, len(nodes))
	for _, node := range nodes {
		if incoming[node.Name] == 0 {
			sources = append(sources, node.Name)
		}
		if outgoing[node.Name] == 0 {
			leaves = append(leaves, node.Name)
		}
	}

	return sources, leaves
}

func graphNodeIDs(nodes []GraphNodeSnapshot) map[string]string {
	ids := make(map[string]string, len(nodes))
	for i, node := range nodes {
		ids[node.Name] = "n" + strconv.Itoa(i)
	}

	return ids
}

func nodeLabel(node GraphNodeSnapshot) string {
	if node.Label != "" {
		return node.Label
	}

	return node.Name
}

func sortEdges(edges []GraphEdgeSnapshot) {
	sort.Slice(edges, func(i, j int) bool {
		if edges[i].From == edges[j].From {
			return edges[i].To < edges[j].To
		}

		return edges[i].From < edges[j].From
	})
}

func escapeMermaidLabel(label string) string {
	label = strings.ReplaceAll(label, `\`, `\\`)
	label = strings.ReplaceAll(label, `"`, `\"`)
	label = strings.ReplaceAll(label, `[`, `\[`)
	label = strings.ReplaceAll(label, `]`, `\]`)
	label = strings.ReplaceAll(label, "\n", `\n`)

	return label
}
