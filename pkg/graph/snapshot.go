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

package graph

import (
	"sort"
	"strconv"
	"strings"
)

// Snapshot is a handler-free view of a graph.
// Nodes and Edges include explicit virtual START and END terminals for export.
// Sources, Leaves, Entries, and Exits list real handler nodes only.
type Snapshot struct {
	Name    string
	Frozen  bool
	Nodes   []NodeSnapshot
	Edges   []EdgeSnapshot
	Sources []string
	Leaves  []string
	Entries []string
	Exits   []string
}

// NodeSnapshot describes one graph node.
type NodeSnapshot struct {
	Name     string
	Label    string
	Metadata map[string]string
}

// EdgeSnapshot describes one graph dependency edge.
type EdgeSnapshot struct {
	From string
	To   string
}

// Copy returns a deterministic defensive copy of the snapshot.
func (s Snapshot) Copy() Snapshot {
	return copySnapshot(s)
}

// Snapshot returns a deterministic, defensive graph snapshot.
func (g *Graph[T]) Snapshot() Snapshot {
	if g == nil {
		return Snapshot{}
	}

	g.mu.RLock()
	defer g.mu.RUnlock()

	return g.snapshotLocked()
}

func (g *Graph[T]) snapshotLocked() Snapshot {
	processing := g.processingSnapshotLocked()
	if len(processing.Nodes) == 0 {
		return processing
	}

	startEdges, endEdges, entries, exits := g.terminalEdgesLocked()
	nodes := make([]NodeSnapshot, 0, len(processing.Nodes)+2)
	nodes = append(nodes, NodeSnapshot{
		Name:  StartNode,
		Label: StartNode,
	})
	nodes = append(nodes, processing.Nodes...)
	nodes = append(nodes, NodeSnapshot{
		Name:  EndNode,
		Label: EndNode,
	})

	edges := make([]EdgeSnapshot, 0, len(startEdges)+len(processing.Edges)+len(endEdges))
	edges = append(edges, startEdges...)
	edges = append(edges, processing.Edges...)
	edges = append(edges, endEdges...)

	return Snapshot{
		Name:    processing.Name,
		Frozen:  processing.Frozen,
		Nodes:   nodes,
		Edges:   edges,
		Sources: append([]string(nil), processing.Sources...),
		Leaves:  append([]string(nil), processing.Leaves...),
		Entries: entries,
		Exits:   exits,
	}
}

func (g *Graph[T]) processingSnapshotLocked() Snapshot {
	nodes := make([]NodeSnapshot, 0, len(g.nodes))
	for _, node := range g.nodes {
		nodes = append(nodes, NodeSnapshot{
			Name:     node.name,
			Label:    node.label,
			Metadata: copyStringMap(node.metadata),
		})
	}
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].Name < nodes[j].Name
	})

	edges := make([]EdgeSnapshot, 0, len(g.edges))
	for edge := range g.edges {
		if isGraphTerminalEdge(edge) {
			continue
		}
		edges = append(edges, edge)
	}
	sortEdges(edges)

	sources, leaves := graphTerminals(nodes, edges)

	return Snapshot{
		Name:    g.name,
		Frozen:  g.frozen,
		Nodes:   nodes,
		Edges:   edges,
		Sources: sources,
		Leaves:  leaves,
	}
}

func (g *Graph[T]) terminalEdgesLocked() (
	[]EdgeSnapshot,
	[]EdgeSnapshot,
	[]string,
	[]string,
) {
	startEdges := make([]EdgeSnapshot, 0)
	endEdges := make([]EdgeSnapshot, 0)
	entries := make([]string, 0)
	exits := make([]string, 0)
	for edge := range g.edges {
		switch {
		case edge.From == StartNode && !isGraphVirtualNodeName(edge.To):
			startEdges = append(startEdges, edge)
			entries = append(entries, edge.To)
		case edge.To == EndNode && !isGraphVirtualNodeName(edge.From):
			endEdges = append(endEdges, edge)
			exits = append(exits, edge.From)
		}
	}
	sortEdges(startEdges)
	sortEdges(endEdges)
	sort.Strings(entries)
	sort.Strings(exits)

	return startEdges, endEdges, entries, exits
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
	snapshot := g.processingSnapshotLocked()
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
	nodes []NodeSnapshot,
	edges []EdgeSnapshot,
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

func graphUnreachableNodes(snapshot Snapshot, entries []string) []string {
	adjacency := graphAdjacency(snapshot.Edges)
	reachable := graphReachable(entries, adjacency)

	missing := make([]string, 0)
	for _, node := range snapshot.Nodes {
		if !reachable[node.Name] {
			missing = append(missing, node.Name)
		}
	}

	return missing
}

func graphNodesCannotReachExits(snapshot Snapshot, exits []string) []string {
	reverse := make(map[string][]string, len(snapshot.Nodes))
	for _, edge := range snapshot.Edges {
		reverse[edge.To] = append(reverse[edge.To], edge.From)
	}
	for node := range reverse {
		sort.Strings(reverse[node])
	}
	reachable := graphReachable(exits, reverse)

	missing := make([]string, 0)
	for _, node := range snapshot.Nodes {
		if !reachable[node.Name] {
			missing = append(missing, node.Name)
		}
	}

	return missing
}

func graphAdjacency(edges []EdgeSnapshot) map[string][]string {
	adjacency := make(map[string][]string)
	for _, edge := range edges {
		adjacency[edge.From] = append(adjacency[edge.From], edge.To)
	}
	for node := range adjacency {
		sort.Strings(adjacency[node])
	}

	return adjacency
}

func graphReachable(entries []string, adjacency map[string][]string) map[string]bool {
	reachable := make(map[string]bool)
	queue := append([]string(nil), entries...)
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		if reachable[current] {
			continue
		}
		reachable[current] = true
		queue = append(queue, adjacency[current]...)
	}

	return reachable
}

func sameStringSet(left []string, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}

	return true
}

func graphNodeIDs(nodes []NodeSnapshot) map[string]string {
	ids := make(map[string]string, len(nodes))
	for i, node := range nodes {
		ids[node.Name] = "n" + strconv.Itoa(i)
	}

	return ids
}

func nodeLabel(node NodeSnapshot) string {
	if node.Label != "" {
		return node.Label
	}

	return node.Name
}

func sortEdges(edges []EdgeSnapshot) {
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

func copySnapshot(snapshot Snapshot) Snapshot {
	nodes := make([]NodeSnapshot, len(snapshot.Nodes))
	for i, node := range snapshot.Nodes {
		nodes[i] = NodeSnapshot{
			Name:     node.Name,
			Label:    node.Label,
			Metadata: copyStringMap(node.Metadata),
		}
	}

	edges := append([]EdgeSnapshot(nil), snapshot.Edges...)
	sources := append([]string(nil), snapshot.Sources...)
	leaves := append([]string(nil), snapshot.Leaves...)
	entries := append([]string(nil), snapshot.Entries...)
	exits := append([]string(nil), snapshot.Exits...)

	return Snapshot{
		Name:    snapshot.Name,
		Frozen:  snapshot.Frozen,
		Nodes:   nodes,
		Edges:   edges,
		Sources: sources,
		Leaves:  leaves,
		Entries: entries,
		Exits:   exits,
	}
}
