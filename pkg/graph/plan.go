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
	"fmt"
	"sort"

	"github.com/photowey/disruptor.go/pkg/event"
)

// Plan is an immutable scheduling view of a validated graph.
type Plan[T any] struct {
	Name       string
	Snapshot   Snapshot
	Processing Snapshot
	Nodes      map[string]PlanNode[T]
	Order      []string
	Upstream   map[string][]string
	Leaves     map[string]bool
}

// PlanNode describes a real handler node in a scheduling plan.
type PlanNode[T any] struct {
	Name             string
	Handler          event.Handler[T]
	ExceptionHandler event.ExceptionHandler[T]
	Label            string
	Metadata         map[string]string
}

// BuildPlan validates, freezes, and returns a scheduling plan.
func (g *Graph[T]) BuildPlan() (Plan[T], error) {
	if g == nil {
		return Plan[T]{}, fmt.Errorf("%w: graph is nil", ErrInvalid)
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if g.handled {
		return Plan[T]{}, ErrHandled
	}
	if err := g.validateLocked(); err != nil {
		return Plan[T]{}, err
	}

	g.freezeHandledLocked()

	processing := g.processingSnapshotLocked()
	snapshot := g.snapshotLocked()
	nodes := make(map[string]PlanNode[T], len(g.nodes))
	for name, node := range g.nodes {
		nodes[name] = PlanNode[T]{
			Name:             node.name,
			Handler:          node.handler,
			ExceptionHandler: node.exceptionHandler,
			Label:            node.label,
			Metadata:         copyStringMap(node.metadata),
		}
	}

	return Plan[T]{
		Name:       g.name,
		Snapshot:   snapshot,
		Processing: processing,
		Nodes:      nodes,
		Order:      topologicalOrder(processing),
		Upstream:   upstreamByNode(processing.Edges),
		Leaves:     nameSet(processing.Leaves),
	}, nil
}

func topologicalOrder(snapshot Snapshot) []string {
	inDegree := make(map[string]int, len(snapshot.Nodes))
	adjacency := make(map[string][]string, len(snapshot.Nodes))
	for _, node := range snapshot.Nodes {
		inDegree[node.Name] = 0
	}
	for _, edge := range snapshot.Edges {
		adjacency[edge.From] = append(adjacency[edge.From], edge.To)
		inDegree[edge.To]++
	}
	for node := range adjacency {
		sort.Strings(adjacency[node])
	}

	ready := make([]string, 0, len(snapshot.Nodes))
	for _, node := range snapshot.Nodes {
		if inDegree[node.Name] == 0 {
			ready = append(ready, node.Name)
		}
	}
	sort.Strings(ready)

	order := make([]string, 0, len(snapshot.Nodes))
	for len(ready) > 0 {
		name := ready[0]
		ready = ready[1:]
		order = append(order, name)
		for _, next := range adjacency[name] {
			inDegree[next]--
			if inDegree[next] == 0 {
				ready = append(ready, next)
				sort.Strings(ready)
			}
		}
	}

	return order
}

func upstreamByNode(edges []EdgeSnapshot) map[string][]string {
	upstream := make(map[string][]string)
	for _, edge := range edges {
		upstream[edge.To] = append(upstream[edge.To], edge.From)
	}
	for name := range upstream {
		sort.Strings(upstream[name])
	}

	return upstream
}

func nameSet(names []string) map[string]bool {
	set := make(map[string]bool, len(names))
	for _, name := range names {
		set[name] = true
	}

	return set
}
