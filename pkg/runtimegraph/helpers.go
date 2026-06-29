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
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"github.com/photowey/disruptor.go/pkg/graph"
)

func normalizeGraphName(kind string, name string) (string, error) {
	normalized := strings.TrimSpace(name)
	if normalized == "" {
		return "", fmt.Errorf("%w: %s name is empty", ErrInvalid, kind)
	}
	for _, ch := range normalized {
		if unicode.IsControl(ch) {
			return "", fmt.Errorf(
				"%w: %s name %q contains a control character",
				ErrInvalid,
				kind,
				normalized,
			)
		}
	}

	return normalized, nil
}

func isGraphVirtualNodeName(name string) bool {
	return name == graph.StartNode || name == graph.EndNode
}

func isGraphStartNodeName(name string) bool {
	return name == graph.StartNode
}

func isGraphEndNodeName(name string) bool {
	return name == graph.EndNode
}

func isGraphTerminalEdge(edge graph.EdgeSnapshot) bool {
	return edge.From == graph.StartNode || edge.To == graph.EndNode
}

func graphTerminals(
	nodes []graph.NodeSnapshot,
	edges []graph.EdgeSnapshot,
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

func graphUnreachableNodes(snapshot graph.Snapshot, entries []string) []string {
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

func graphNodesCannotReachExits(snapshot graph.Snapshot, exits []string) []string {
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

func graphAdjacency(edges []graph.EdgeSnapshot) map[string][]string {
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

func graphNodeIDs(nodes []graph.NodeSnapshot) map[string]string {
	ids := make(map[string]string, len(nodes))
	for i, node := range nodes {
		ids[node.Name] = "n" + strconv.Itoa(i)
	}

	return ids
}

func nodeLabel(node graph.NodeSnapshot) string {
	if node.Label != "" {
		return node.Label
	}

	return node.Name
}

func sortEdges(edges []graph.EdgeSnapshot) {
	sort.Sort(edgeSnapshotsByEndpoints(edges))
}

type nodeSnapshotsByName []graph.NodeSnapshot

func (nodes nodeSnapshotsByName) Len() int {
	return len(nodes)
}

func (nodes nodeSnapshotsByName) Less(left int, right int) bool {
	return nodes[left].Name < nodes[right].Name
}

func (nodes nodeSnapshotsByName) Swap(left int, right int) {
	nodes[left], nodes[right] = nodes[right], nodes[left]
}

type edgeSnapshotsByEndpoints []graph.EdgeSnapshot

func (edges edgeSnapshotsByEndpoints) Len() int {
	return len(edges)
}

func (edges edgeSnapshotsByEndpoints) Less(left int, right int) bool {
	if edges[left].From == edges[right].From {
		return edges[left].To < edges[right].To
	}

	return edges[left].From < edges[right].From
}

func (edges edgeSnapshotsByEndpoints) Swap(left int, right int) {
	edges[left], edges[right] = edges[right], edges[left]
}

func escapeMermaidLabel(label string) string {
	label = strings.ReplaceAll(label, `\`, `\\`)
	label = strings.ReplaceAll(label, `"`, `\"`)
	label = strings.ReplaceAll(label, `[`, `\[`)
	label = strings.ReplaceAll(label, `]`, `\]`)
	label = strings.ReplaceAll(label, "\n", `\n`)

	return label
}

func copyStringMap(input map[string]string) map[string]string {
	if len(input) == 0 {
		return nil
	}

	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}

	return output
}
