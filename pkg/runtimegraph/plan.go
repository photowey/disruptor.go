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

	"github.com/photowey/disruptor.go/pkg/event"
	"github.com/photowey/disruptor.go/pkg/expression"
	"github.com/photowey/disruptor.go/pkg/graph"
)

// Plan is an immutable scheduling view of a validated runtime graph.
type Plan[T any] struct {
	Name     string
	Snapshot RuntimeGraphSnapshot
	Nodes    map[string]*PlanNode[T]
	Start    []PlanEdge[T]
}

// PlanNode describes one real handler node in a runtime graph plan.
type PlanNode[T any] struct {
	Name             string
	Handler          event.Handler[T]
	ExceptionHandler event.ExceptionHandler[T]
	Label            string
	Incoming         int
	Outgoing         []PlanEdge[T]
}

// PlanEdge describes one runtime graph routing edge.
type PlanEdge[T any] struct {
	From              string
	To                string
	Condition         EdgeCondition[T]
	CompiledCondition expression.BoolExpression
}

// Evaluate returns whether the edge is selected for the current event.
func (e PlanEdge[T]) Evaluate(request EdgeConditionRequest[T]) (bool, error) {
	if e.CompiledCondition != nil {
		return e.CompiledCondition.EvaluateBool(expression.Request{
			Context:   request.Context,
			Variables: request.Runtime.Variables(),
		})
	}
	if e.Condition == nil {
		return true, nil
	}

	return e.Condition.Evaluate(request)
}

// BuildPlan validates, freezes, and returns a scheduling plan.
func (g *RuntimeGraph[T]) BuildPlan() (*Plan[T], error) {
	if g == nil {
		return nil, fmt.Errorf("%w: runtime graph is nil", ErrInvalid)
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if g.handled {
		return nil, ErrHandled
	}
	if err := g.validateLocked(); err != nil {
		return nil, err
	}

	g.freezeHandledLocked()

	nodes := make(map[string]*PlanNode[T], len(g.nodes))
	for name, node := range g.nodes {
		nodes[name] = &PlanNode[T]{
			Name:             node.name,
			Handler:          node.handler,
			ExceptionHandler: node.exceptionHandler,
			Label:            node.label,
		}
	}

	edgesByFrom := make(map[string][]PlanEdge[T])
	startEdges := make([]PlanEdge[T], 0)
	for key, edge := range g.edges {
		planEdge := PlanEdge[T]{
			From:              key.From,
			To:                key.To,
			Condition:         edge.condition,
			CompiledCondition: edge.compiledCondition,
		}
		if key.From == graph.StartNode {
			startEdges = append(startEdges, planEdge)
		} else {
			edgesByFrom[key.From] = append(edgesByFrom[key.From], planEdge)
		}
		if key.To != graph.EndNode {
			nodes[key.To].Incoming++
		}
	}
	sort.Slice(startEdges, func(i, j int) bool {
		return startEdges[i].To < startEdges[j].To
	})
	for from := range edgesByFrom {
		sort.Slice(edgesByFrom[from], func(i, j int) bool {
			return edgesByFrom[from][i].To < edgesByFrom[from][j].To
		})
		nodes[from].Outgoing = append(nodes[from].Outgoing, edgesByFrom[from]...)
	}
	for _, node := range nodes {
		if node.Incoming == 0 {
			// Validation guarantees at least one incoming edge; this keeps the
			// scheduler safe if a future validation rule changes.
			node.Incoming = 1
		}
	}

	return &Plan[T]{
		Name:     g.name,
		Snapshot: g.snapshotLocked(),
		Nodes:    nodes,
		Start:    startEdges,
	}, nil
}
