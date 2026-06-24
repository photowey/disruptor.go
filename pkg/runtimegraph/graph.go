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
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/photowey/disruptor.go/pkg/event"
	"github.com/photowey/disruptor.go/pkg/expression"
	"github.com/photowey/disruptor.go/pkg/graph"
	"github.com/photowey/disruptor.go/pkg/runtimevars"
)

// EdgeCondition decides whether one runtime graph edge is selected.
type EdgeCondition[T any] interface {
	Evaluate(request EdgeConditionRequest[T]) (bool, error)
}

// EdgeConditionFunc adapts a function to EdgeCondition.
type EdgeConditionFunc[T any] func(EdgeConditionRequest[T]) (bool, error)

// Evaluate calls the wrapped edge condition function.
func (fn EdgeConditionFunc[T]) Evaluate(
	request EdgeConditionRequest[T],
) (bool, error) {
	if fn == nil {
		return false, fmt.Errorf("%w: edge condition is nil", ErrInvalid)
	}

	return fn(request)
}

// EdgeConditionRequest describes one runtime edge condition evaluation.
type EdgeConditionRequest[T any] struct {
	Context   context.Context
	Event     *T
	Sequence  int64
	GraphName string
	From      string
	To        string
	Runtime   runtimevars.ContextView
}

// RuntimeGraph defines a conditional event routing topology.
type RuntimeGraph[T any] struct {
	mu sync.RWMutex

	name     string
	nodes    map[string]*runtimeNode[T]
	edges    map[graph.EdgeSnapshot]*runtimeGraphEdge[T]
	compiler expression.Compiler
	frozen   bool
	handled  bool
}

type runtimeNode[T any] struct {
	name             string
	handler          event.Handler[T]
	exceptionHandler event.ExceptionHandler[T]
	label            string
	metadata         map[string]string
}

type nodeConfig[T any] struct {
	exceptionHandler event.ExceptionHandler[T]
	label            string
	metadata         map[string]string
}

type runtimeGraphEdge[T any] struct {
	from              string
	to                string
	condition         EdgeCondition[T]
	conditionLabel    string
	compiledCondition expression.BoolExpression
}

// RuntimeGraphOption configures a RuntimeGraph builder.
type RuntimeGraphOption interface {
	applyRuntimeGraph(config *runtimeGraphConfig) error
}

type runtimeGraphConfig struct {
	compiler expression.Compiler
}

type runtimeGraphOptionFunc struct {
	applyFunc func(*runtimeGraphConfig) error
}

//nolint:unused // The method satisfies RuntimeGraphOption and is called through the interface.
func (fn runtimeGraphOptionFunc) applyRuntimeGraph(config *runtimeGraphConfig) error {
	return fn.applyFunc(config)
}

// WithExpressionCompiler replaces the graph expression compiler.
func WithExpressionCompiler(compiler expression.Compiler) RuntimeGraphOption {
	return runtimeGraphOptionFunc{
		applyFunc: func(config *runtimeGraphConfig) error {
			if compiler == nil {
				return fmt.Errorf("%w: runtime expression compiler is nil", ErrInvalid)
			}

			config.compiler = compiler
			return nil
		},
	}
}

// RuntimeNodeOption configures one runtime graph node.
type RuntimeNodeOption[T any] interface {
	applyNode(config *nodeConfig[T]) error
}

type runtimeNodeOptionFunc[T any] struct {
	applyFunc func(*nodeConfig[T]) error
}

//nolint:unused // The method satisfies RuntimeNodeOption[T] and is called through the interface.
func (fn runtimeNodeOptionFunc[T]) applyNode(config *nodeConfig[T]) error {
	return fn.applyFunc(config)
}

// WithNodeExceptionHandler sets the exception handler for one runtime graph node.
func WithNodeExceptionHandler[T any](handler event.ExceptionHandler[T]) RuntimeNodeOption[T] {
	return runtimeNodeOptionFunc[T]{
		applyFunc: func(config *nodeConfig[T]) error {
			if handler == nil {
				return fmt.Errorf("%w: runtime node exception handler is nil", ErrInvalid)
			}

			config.exceptionHandler = handler
			return nil
		},
	}
}

// WithNodeLabel sets the display label for one runtime graph node.
func WithNodeLabel[T any](label string) RuntimeNodeOption[T] {
	return runtimeNodeOptionFunc[T]{
		applyFunc: func(config *nodeConfig[T]) error {
			normalized, err := normalizeGraphName("node label", label)
			if err != nil {
				return err
			}

			config.label = normalized
			return nil
		},
	}
}

// WithNodeMetadata adds one metadata key-value pair to a runtime graph node.
func WithNodeMetadata[T any](key string, value string) RuntimeNodeOption[T] {
	return runtimeNodeOptionFunc[T]{
		applyFunc: func(config *nodeConfig[T]) error {
			normalizedKey, err := normalizeGraphName("metadata key", key)
			if err != nil {
				return err
			}
			normalizedValue, err := normalizeGraphName("metadata value", value)
			if err != nil {
				return err
			}

			if config.metadata == nil {
				config.metadata = make(map[string]string)
			}
			config.metadata[normalizedKey] = normalizedValue
			return nil
		},
	}
}

// RuntimeEdgeOption configures one runtime graph edge.
type RuntimeEdgeOption[T any] interface {
	applyRuntimeEdge(config *runtimeEdgeConfig[T]) error
}

type runtimeEdgeConfig[T any] struct {
	condition      EdgeCondition[T]
	conditionLabel string
	expression     expression.Expression
	hasExpression  bool
}

type runtimeEdgeOptionFunc[T any] struct {
	applyFunc func(*runtimeEdgeConfig[T]) error
}

//nolint:unused // The method satisfies RuntimeEdgeOption[T] and is called through the interface.
func (fn runtimeEdgeOptionFunc[T]) applyRuntimeEdge(config *runtimeEdgeConfig[T]) error {
	return fn.applyFunc(config)
}

// WhenCondition sets a typed runtime graph edge condition.
func WhenCondition[T any](condition EdgeCondition[T]) RuntimeEdgeOption[T] {
	return runtimeEdgeOptionFunc[T]{
		applyFunc: func(config *runtimeEdgeConfig[T]) error {
			if condition == nil {
				return fmt.Errorf("%w: runtime edge condition is nil", ErrInvalid)
			}

			config.condition = condition
			config.conditionLabel = "custom"
			config.hasExpression = false
			return nil
		},
	}
}

// WhenExpression sets an expression runtime graph edge condition.
func WhenExpression[T any](runtimeExpression expression.Expression) RuntimeEdgeOption[T] {
	return runtimeEdgeOptionFunc[T]{
		applyFunc: func(config *runtimeEdgeConfig[T]) error {
			if strings.TrimSpace(string(runtimeExpression)) == "" {
				return fmt.Errorf("%w: runtime edge expression is empty", ErrInvalid)
			}

			config.expression = runtimeExpression
			config.conditionLabel = string(runtimeExpression)
			config.hasExpression = true
			config.condition = nil
			return nil
		},
	}
}

// NewRuntimeGraph creates a mutable runtime graph builder.
func NewRuntimeGraph[T any](name string, opts ...RuntimeGraphOption) (*RuntimeGraph[T], error) {
	normalized, err := normalizeGraphName("runtime graph", name)
	if err != nil {
		return nil, err
	}

	config := runtimeGraphConfig{
		compiler: expression.NewCompiler(),
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt.applyRuntimeGraph(&config); err != nil {
			return nil, fmt.Errorf("applying runtime graph option: %w", err)
		}
	}

	return &RuntimeGraph[T]{
		name:     normalized,
		nodes:    make(map[string]*runtimeNode[T]),
		edges:    make(map[graph.EdgeSnapshot]*runtimeGraphEdge[T]),
		compiler: config.compiler,
	}, nil
}

// MustRuntimeGraph creates a runtime graph builder or panics.
func MustRuntimeGraph[T any](name string, opts ...RuntimeGraphOption) *RuntimeGraph[T] {
	graph, err := NewRuntimeGraph[T](name, opts...)
	if err != nil {
		panic(err)
	}

	return graph
}

// Name returns the runtime graph name.
func (g *RuntimeGraph[T]) Name() string {
	if g == nil {
		return ""
	}

	g.mu.RLock()
	defer g.mu.RUnlock()

	return g.name
}

// Node registers a runtime graph handler node.
func (g *RuntimeGraph[T]) Node(
	name string,
	handler event.Handler[T],
	opts ...RuntimeNodeOption[T],
) error {
	if g == nil {
		return fmt.Errorf("%w: runtime graph is nil", ErrInvalid)
	}
	if handler == nil {
		return fmt.Errorf("%w: runtime graph %s: node handler is nil", ErrInvalid, g.Name())
	}

	normalized, err := normalizeGraphName("node", name)
	if err != nil {
		return err
	}
	if isGraphVirtualNodeName(normalized) {
		return fmt.Errorf(
			"%w: runtime graph %s: node name %s is reserved",
			ErrInvalid,
			g.Name(),
			normalized,
		)
	}

	config := nodeConfig[T]{
		label:    normalized,
		metadata: make(map[string]string),
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt.applyNode(&config); err != nil {
			return fmt.Errorf("applying runtime node option: %w", err)
		}
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if g.frozen {
		return ErrFrozen
	}
	if _, exists := g.nodes[normalized]; exists {
		return fmt.Errorf(
			"%w: runtime graph %s: node %s already exists",
			ErrInvalid,
			g.name,
			normalized,
		)
	}

	g.nodes[normalized] = &runtimeNode[T]{
		name:             normalized,
		handler:          handler,
		exceptionHandler: config.exceptionHandler,
		label:            config.label,
		metadata:         copyStringMap(config.metadata),
	}

	return nil
}

// MustNode registers a runtime graph node or panics.
func (g *RuntimeGraph[T]) MustNode(
	name string,
	handler event.Handler[T],
	opts ...RuntimeNodeOption[T],
) *RuntimeGraph[T] {
	if err := g.Node(name, handler, opts...); err != nil {
		panic(err)
	}

	return g
}

// Edge registers a conditional runtime graph edge.
func (g *RuntimeGraph[T]) Edge(
	from string,
	to string,
	opts ...RuntimeEdgeOption[T],
) error {
	if g == nil {
		return fmt.Errorf("%w: runtime graph is nil", ErrInvalid)
	}

	normalizedFrom, err := normalizeGraphName("node", from)
	if err != nil {
		return err
	}
	normalizedTo, err := normalizeGraphName("node", to)
	if err != nil {
		return err
	}

	config := runtimeEdgeConfig[T]{
		condition:      runtimeTrueCondition[T]{},
		conditionLabel: "true",
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt.applyRuntimeEdge(&config); err != nil {
			return fmt.Errorf("applying runtime edge option: %w", err)
		}
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if g.frozen {
		return ErrFrozen
	}
	if err := g.validateEdgeLocked(normalizedFrom, normalizedTo); err != nil {
		return err
	}

	var compiled expression.BoolExpression
	if config.hasExpression {
		compiled, err = g.compiler.Compile(config.expression)
		if err != nil {
			return fmt.Errorf("compiling runtime edge expression: %w", err)
		}
	}

	g.edges[graph.EdgeSnapshot{From: normalizedFrom, To: normalizedTo}] = &runtimeGraphEdge[T]{
		from:              normalizedFrom,
		to:                normalizedTo,
		condition:         config.condition,
		conditionLabel:    config.conditionLabel,
		compiledCondition: compiled,
	}

	return nil
}

// MustEdge registers a runtime graph edge or panics.
func (g *RuntimeGraph[T]) MustEdge(
	from string,
	to string,
	opts ...RuntimeEdgeOption[T],
) *RuntimeGraph[T] {
	if err := g.Edge(from, to, opts...); err != nil {
		panic(err)
	}

	return g
}

func (g *RuntimeGraph[T]) validateEdgeLocked(from string, to string) error {
	if isGraphStartNodeName(from) {
		if isGraphVirtualNodeName(to) {
			return fmt.Errorf(
				"%w: runtime graph %s: invalid terminal edge %s -> %s",
				ErrInvalid,
				g.name,
				from,
				to,
			)
		}
		if _, exists := g.nodes[to]; !exists {
			return fmt.Errorf(
				"%w: runtime graph %s: edge %s -> %s references unknown node %s",
				ErrInvalid,
				g.name,
				from,
				to,
				to,
			)
		}

		return nil
	}
	if isGraphEndNodeName(from) || isGraphStartNodeName(to) {
		return fmt.Errorf(
			"%w: runtime graph %s: invalid terminal edge %s -> %s",
			ErrInvalid,
			g.name,
			from,
			to,
		)
	}
	if isGraphEndNodeName(to) {
		if _, exists := g.nodes[from]; !exists {
			return fmt.Errorf(
				"%w: runtime graph %s: edge %s -> %s references unknown node %s",
				ErrInvalid,
				g.name,
				from,
				to,
				from,
			)
		}

		return nil
	}
	if from == to {
		return fmt.Errorf(
			"%w: runtime graph %s: self edge %s -> %s",
			ErrInvalid,
			g.name,
			from,
			to,
		)
	}
	if _, exists := g.nodes[from]; !exists {
		return fmt.Errorf(
			"%w: runtime graph %s: edge %s -> %s references unknown node %s",
			ErrInvalid,
			g.name,
			from,
			to,
			from,
		)
	}
	if _, exists := g.nodes[to]; !exists {
		return fmt.Errorf(
			"%w: runtime graph %s: edge %s -> %s references unknown node %s",
			ErrInvalid,
			g.name,
			from,
			to,
			to,
		)
	}

	return nil
}

// Validate checks whether the runtime graph can be scheduled.
func (g *RuntimeGraph[T]) Validate() error {
	if g == nil {
		return fmt.Errorf("%w: runtime graph is nil", ErrInvalid)
	}

	g.mu.RLock()
	defer g.mu.RUnlock()

	return g.validateLocked()
}

func (g *RuntimeGraph[T]) validateLocked() error {
	if len(g.nodes) == 0 {
		return fmt.Errorf("%w: runtime graph %s: no nodes", ErrInvalid, g.name)
	}
	if cycle := g.findCycleLocked(); len(cycle) > 0 {
		return fmt.Errorf(
			"%w: runtime graph %s: cycle detected: %s",
			ErrInvalid,
			g.name,
			strings.Join(cycle, " -> "),
		)
	}

	publicSnapshot := g.snapshotLocked()
	processingSnapshot := g.processingSnapshotLocked()
	if len(publicSnapshot.Entries) == 0 {
		return fmt.Errorf("%w: runtime graph %s: no explicit entry edges", ErrInvalid, g.name)
	}
	if len(publicSnapshot.Exits) == 0 {
		return fmt.Errorf("%w: runtime graph %s: no explicit exit edges", ErrInvalid, g.name)
	}
	if !sameStringSet(publicSnapshot.Entries, publicSnapshot.Sources) {
		return fmt.Errorf(
			"%w: runtime graph %s: entries must match sources: entries=%v sources=%v",
			ErrInvalid,
			g.name,
			publicSnapshot.Entries,
			publicSnapshot.Sources,
		)
	}
	if !sameStringSet(publicSnapshot.Exits, publicSnapshot.Leaves) {
		return fmt.Errorf(
			"%w: runtime graph %s: exits must match leaves: exits=%v leaves=%v",
			ErrInvalid,
			g.name,
			publicSnapshot.Exits,
			publicSnapshot.Leaves,
		)
	}
	if unreachable := graphUnreachableNodes(processingSnapshot, publicSnapshot.Entries); len(unreachable) > 0 {
		return fmt.Errorf(
			"%w: runtime graph %s: nodes are not reachable from START: %s",
			ErrInvalid,
			g.name,
			strings.Join(unreachable, ", "),
		)
	}
	if blocked := graphNodesCannotReachExits(processingSnapshot, publicSnapshot.Exits); len(blocked) > 0 {
		return fmt.Errorf(
			"%w: runtime graph %s: nodes cannot reach END: %s",
			ErrInvalid,
			g.name,
			strings.Join(blocked, ", "),
		)
	}

	return nil
}

func (g *RuntimeGraph[T]) freezeHandledLocked() {
	g.frozen = true
	g.handled = true
}

func (g *RuntimeGraph[T]) findCycleLocked() []string {
	snapshot := g.processingSnapshotLocked()
	adjacency := graphAdjacency(snapshot.Edges)
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

// RuntimeGraphSnapshot is a handler-free view of a runtime graph.
type RuntimeGraphSnapshot struct {
	Name    string
	Frozen  bool
	Nodes   []graph.NodeSnapshot
	Edges   []RuntimeGraphEdgeSnapshot
	Sources []string
	Leaves  []string
	Entries []string
	Exits   []string
}

// RuntimeGraphEdgeSnapshot describes one runtime graph routing edge.
type RuntimeGraphEdgeSnapshot struct {
	From      string
	To        string
	Condition string
}

// Snapshot returns a deterministic, defensive runtime graph snapshot.
func (g *RuntimeGraph[T]) Snapshot() RuntimeGraphSnapshot {
	if g == nil {
		return RuntimeGraphSnapshot{}
	}

	g.mu.RLock()
	defer g.mu.RUnlock()

	return g.snapshotLocked()
}

func (g *RuntimeGraph[T]) snapshotLocked() RuntimeGraphSnapshot {
	processing := g.processingSnapshotLocked()
	if len(processing.Nodes) == 0 {
		return runtimeSnapshotFromProcessing(processing, nil, nil, nil, nil)
	}

	startEdges, endEdges, entries, exits := g.terminalEdgeSnapshotsLocked()
	nodes := make([]graph.NodeSnapshot, 0, len(processing.Nodes)+2)
	nodes = append(nodes, graph.NodeSnapshot{Name: graph.StartNode, Label: graph.StartNode})
	nodes = append(nodes, processing.Nodes...)
	nodes = append(nodes, graph.NodeSnapshot{Name: graph.EndNode, Label: graph.EndNode})

	edges := make([]RuntimeGraphEdgeSnapshot, 0, len(startEdges)+len(processing.Edges)+len(endEdges))
	edges = append(edges, startEdges...)
	edges = append(edges, runtimeEdgeSnapshotsFromGraphEdges(processing.Edges, g.edges)...)
	edges = append(edges, endEdges...)

	return RuntimeGraphSnapshot{
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

func (g *RuntimeGraph[T]) processingSnapshotLocked() graph.Snapshot {
	nodes := make([]graph.NodeSnapshot, 0, len(g.nodes))
	for _, node := range g.nodes {
		nodes = append(nodes, graph.NodeSnapshot{
			Name:     node.name,
			Label:    node.label,
			Metadata: copyStringMap(node.metadata),
		})
	}
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].Name < nodes[j].Name
	})

	edges := make([]graph.EdgeSnapshot, 0, len(g.edges))
	for edge := range g.edges {
		if isGraphTerminalEdge(edge) {
			continue
		}
		edges = append(edges, edge)
	}
	sortEdges(edges)
	sources, leaves := graphTerminals(nodes, edges)

	return graph.Snapshot{
		Name:    g.name,
		Frozen:  g.frozen,
		Nodes:   nodes,
		Edges:   edges,
		Sources: sources,
		Leaves:  leaves,
	}
}

func (g *RuntimeGraph[T]) terminalEdgeSnapshotsLocked() (
	[]RuntimeGraphEdgeSnapshot,
	[]RuntimeGraphEdgeSnapshot,
	[]string,
	[]string,
) {
	startEdges := make([]RuntimeGraphEdgeSnapshot, 0)
	endEdges := make([]RuntimeGraphEdgeSnapshot, 0)
	entries := make([]string, 0)
	exits := make([]string, 0)
	for key, edge := range g.edges {
		switch {
		case key.From == graph.StartNode && !isGraphVirtualNodeName(key.To):
			startEdges = append(startEdges, edge.snapshot())
			entries = append(entries, key.To)
		case key.To == graph.EndNode && !isGraphVirtualNodeName(key.From):
			endEdges = append(endEdges, edge.snapshot())
			exits = append(exits, key.From)
		}
	}
	sortRuntimeEdges(startEdges)
	sortRuntimeEdges(endEdges)
	sort.Strings(entries)
	sort.Strings(exits)

	return startEdges, endEdges, entries, exits
}

func runtimeEdgeSnapshotsFromGraphEdges[T any](
	edges []graph.EdgeSnapshot,
	edgeByKey map[graph.EdgeSnapshot]*runtimeGraphEdge[T],
) []RuntimeGraphEdgeSnapshot {
	snapshots := make([]RuntimeGraphEdgeSnapshot, 0, len(edges))
	for _, edge := range edges {
		snapshots = append(snapshots, edgeByKey[edge].snapshot())
	}

	return snapshots
}

func runtimeSnapshotFromProcessing(
	processing graph.Snapshot,
	edges []RuntimeGraphEdgeSnapshot,
	entries []string,
	exits []string,
	nodes []graph.NodeSnapshot,
) RuntimeGraphSnapshot {
	if nodes == nil {
		nodes = processing.Nodes
	}

	return RuntimeGraphSnapshot{
		Name:    processing.Name,
		Frozen:  processing.Frozen,
		Nodes:   nodes,
		Edges:   edges,
		Sources: append([]string(nil), processing.Sources...),
		Leaves:  append([]string(nil), processing.Leaves...),
		Entries: append([]string(nil), entries...),
		Exits:   append([]string(nil), exits...),
	}
}

func (e *runtimeGraphEdge[T]) snapshot() RuntimeGraphEdgeSnapshot {
	if e == nil {
		return RuntimeGraphEdgeSnapshot{}
	}

	return RuntimeGraphEdgeSnapshot{
		From:      e.from,
		To:        e.to,
		Condition: e.conditionLabel,
	}
}

func sortRuntimeEdges(edges []RuntimeGraphEdgeSnapshot) {
	sort.Slice(edges, func(i, j int) bool {
		if edges[i].From == edges[j].From {
			return edges[i].To < edges[j].To
		}

		return edges[i].From < edges[j].From
	})
}

// Mermaid renders the runtime graph as a Mermaid flowchart.
func (g *RuntimeGraph[T]) Mermaid() string {
	snapshot := g.Snapshot()
	ids := graphNodeIDs(snapshot.Nodes)

	var builder strings.Builder
	builder.WriteString("flowchart LR\n")
	for _, node := range snapshot.Nodes {
		builder.WriteString("    ")
		builder.WriteString(ids[node.Name])
		builder.WriteString("[\"")
		builder.WriteString(escapeMermaidLabel(nodeLabel(node)))
		builder.WriteString("\"]\n")
	}
	for _, edge := range snapshot.Edges {
		builder.WriteString("    ")
		builder.WriteString(ids[edge.From])
		builder.WriteString(" -->|")
		builder.WriteString(escapeMermaidLabel(edge.Condition))
		builder.WriteString("| ")
		builder.WriteString(ids[edge.To])
		builder.WriteString("\n")
	}

	return builder.String()
}

// DOT renders the runtime graph as a Graphviz DOT graph.
func (g *RuntimeGraph[T]) DOT() string {
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
		builder.WriteString(" [label=")
		builder.WriteString(strconv.Quote(edge.Condition))
		builder.WriteString("];\n")
	}
	builder.WriteString("}\n")

	return builder.String()
}

type runtimeTrueCondition[T any] struct{}

func (runtimeTrueCondition[T]) Evaluate(
	request EdgeConditionRequest[T],
) (bool, error) {
	return true, nil
}
