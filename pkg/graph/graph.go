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
	"strings"
	"sync"
	"unicode"

	"github.com/photowey/disruptor.go/pkg/event"
)

const (
	// StartNode is the reserved virtual node name for graph entry points.
	StartNode = "START"
	// EndNode is the reserved virtual node name for graph exit points.
	EndNode = "END"
)

// Graph defines a named event processor dependency topology.
type Graph[T any] struct {
	mu sync.RWMutex

	name    string
	nodes   map[string]*graphNode[T]
	edges   map[EdgeSnapshot]struct{}
	frozen  bool
	handled bool
}

type graphNode[T any] struct {
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

// NodeOption configures one graph node.
type NodeOption[T any] func(config *nodeConfig[T]) error

// New creates a mutable graph builder.
func New[T any](name string) (*Graph[T], error) {
	normalized, err := normalizeGraphName("graph", name)
	if err != nil {
		return nil, err
	}

	return &Graph[T]{
		name:  normalized,
		nodes: make(map[string]*graphNode[T]),
		edges: make(map[EdgeSnapshot]struct{}),
	}, nil
}

// Must creates a graph builder or panics when name is invalid.
func Must[T any](name string) *Graph[T] {
	graph, err := New[T](name)
	if err != nil {
		panic(err)
	}

	return graph
}

// Name returns the graph name.
func (g *Graph[T]) Name() string {
	if g == nil {
		return ""
	}

	g.mu.RLock()
	defer g.mu.RUnlock()

	return g.name
}

// Node registers a named event handler in the graph.
func (g *Graph[T]) Node(
	name string,
	handler event.Handler[T],
	opts ...NodeOption[T],
) error {
	if g == nil {
		return fmt.Errorf("%w: graph is nil", ErrInvalid)
	}
	if handler == nil {
		return fmt.Errorf("%w: graph %s: node handler is nil", ErrInvalid, g.Name())
	}

	normalized, err := normalizeGraphName("node", name)
	if err != nil {
		return err
	}
	if isGraphVirtualNodeName(normalized) {
		return fmt.Errorf(
			"%w: graph %s: node name %s is reserved",
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
		if err := opt(&config); err != nil {
			return fmt.Errorf("applying node option: %w", err)
		}
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if g.frozen {
		return ErrFrozen
	}
	if _, exists := g.nodes[normalized]; exists {
		return fmt.Errorf(
			"%w: graph %s: node %s already exists",
			ErrInvalid,
			g.name,
			normalized,
		)
	}

	g.nodes[normalized] = &graphNode[T]{
		name:             normalized,
		handler:          handler,
		exceptionHandler: config.exceptionHandler,
		label:            config.label,
		metadata:         copyStringMap(config.metadata),
	}

	return nil
}

// MustNode registers a node or panics when registration fails.
func (g *Graph[T]) MustNode(
	name string,
	handler event.Handler[T],
	opts ...NodeOption[T],
) *Graph[T] {
	if err := g.Node(name, handler, opts...); err != nil {
		panic(err)
	}

	return g
}

// Edge registers a dependency from one node to another.
func (g *Graph[T]) Edge(from string, to string) error {
	if g == nil {
		return fmt.Errorf("%w: graph is nil", ErrInvalid)
	}

	normalizedFrom, err := normalizeGraphName("node", from)
	if err != nil {
		return err
	}
	normalizedTo, err := normalizeGraphName("node", to)
	if err != nil {
		return err
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if g.frozen {
		return ErrFrozen
	}
	if err := g.validateEdgeLocked(normalizedFrom, normalizedTo); err != nil {
		return err
	}

	g.edges[EdgeSnapshot{From: normalizedFrom, To: normalizedTo}] = struct{}{}

	return nil
}

func (g *Graph[T]) validateEdgeLocked(from string, to string) error {
	if isGraphStartNodeName(from) {
		if isGraphVirtualNodeName(to) {
			return fmt.Errorf(
				"%w: graph %s: invalid terminal edge %s -> %s",
				ErrInvalid,
				g.name,
				from,
				to,
			)
		}
		if _, exists := g.nodes[to]; !exists {
			return fmt.Errorf(
				"%w: graph %s: edge %s -> %s references unknown node %s",
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
			"%w: graph %s: invalid terminal edge %s -> %s",
			ErrInvalid,
			g.name,
			from,
			to,
		)
	}
	if isGraphEndNodeName(to) {
		if _, exists := g.nodes[from]; !exists {
			return fmt.Errorf(
				"%w: graph %s: edge %s -> %s references unknown node %s",
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
			"%w: graph %s: self edge %s -> %s",
			ErrInvalid,
			g.name,
			from,
			to,
		)
	}
	if _, exists := g.nodes[from]; !exists {
		return fmt.Errorf(
			"%w: graph %s: edge %s -> %s references unknown node %s",
			ErrInvalid,
			g.name,
			from,
			to,
			from,
		)
	}
	if _, exists := g.nodes[to]; !exists {
		return fmt.Errorf(
			"%w: graph %s: edge %s -> %s references unknown node %s",
			ErrInvalid,
			g.name,
			from,
			to,
			to,
		)
	}

	return nil
}

// MustEdge registers an edge or panics when registration fails.
func (g *Graph[T]) MustEdge(from string, to string) *Graph[T] {
	if err := g.Edge(from, to); err != nil {
		panic(err)
	}

	return g
}

// Validate checks whether the graph can be scheduled.
func (g *Graph[T]) Validate() error {
	if g == nil {
		return fmt.Errorf("%w: graph is nil", ErrInvalid)
	}

	g.mu.RLock()
	defer g.mu.RUnlock()

	return g.validateLocked()
}

func (g *Graph[T]) validateLocked() error {
	if len(g.nodes) == 0 {
		return fmt.Errorf("%w: graph %s: no nodes", ErrInvalid, g.name)
	}

	if cycle := g.findCycleLocked(); len(cycle) > 0 {
		return fmt.Errorf(
			"%w: graph %s: cycle detected: %s",
			ErrInvalid,
			g.name,
			strings.Join(cycle, " -> "),
		)
	}

	publicSnapshot := g.snapshotLocked()
	processingSnapshot := g.processingSnapshotLocked()
	if len(publicSnapshot.Entries) == 0 {
		return fmt.Errorf("%w: graph %s: no explicit entry edges", ErrInvalid, g.name)
	}
	if len(publicSnapshot.Exits) == 0 {
		return fmt.Errorf("%w: graph %s: no explicit exit edges", ErrInvalid, g.name)
	}
	if !sameStringSet(publicSnapshot.Entries, publicSnapshot.Sources) {
		return fmt.Errorf(
			"%w: graph %s: entries must match sources: entries=%v sources=%v",
			ErrInvalid,
			g.name,
			publicSnapshot.Entries,
			publicSnapshot.Sources,
		)
	}
	if !sameStringSet(publicSnapshot.Exits, publicSnapshot.Leaves) {
		return fmt.Errorf(
			"%w: graph %s: exits must match leaves: exits=%v leaves=%v",
			ErrInvalid,
			g.name,
			publicSnapshot.Exits,
			publicSnapshot.Leaves,
		)
	}
	if unreachable := graphUnreachableNodes(processingSnapshot, publicSnapshot.Entries); len(unreachable) > 0 {
		return fmt.Errorf(
			"%w: graph %s: nodes are not reachable from START: %s",
			ErrInvalid,
			g.name,
			strings.Join(unreachable, ", "),
		)
	}
	if blocked := graphNodesCannotReachExits(processingSnapshot, publicSnapshot.Exits); len(blocked) > 0 {
		return fmt.Errorf(
			"%w: graph %s: nodes cannot reach END: %s",
			ErrInvalid,
			g.name,
			strings.Join(blocked, ", "),
		)
	}

	return nil
}

func (g *Graph[T]) freezeHandledLocked() {
	g.frozen = true
	g.handled = true
}

// WithNodeExceptionHandler sets the exception handler for one graph node.
func WithNodeExceptionHandler[T any](handler event.ExceptionHandler[T]) NodeOption[T] {
	return func(config *nodeConfig[T]) error {
		if handler == nil {
			return fmt.Errorf("%w: node exception handler is nil", ErrInvalid)
		}

		config.exceptionHandler = handler
		return nil
	}
}

// WithNodeLabel sets the display label for one graph node.
func WithNodeLabel[T any](label string) NodeOption[T] {
	return func(config *nodeConfig[T]) error {
		normalized, err := normalizeGraphName("node label", label)
		if err != nil {
			return err
		}

		config.label = normalized
		return nil
	}
}

// WithNodeMetadata adds one metadata key-value pair to a graph node.
func WithNodeMetadata[T any](key string, value string) NodeOption[T] {
	return func(config *nodeConfig[T]) error {
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
	}
}

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
	return name == StartNode || name == EndNode
}

func isGraphStartNodeName(name string) bool {
	return name == StartNode
}

func isGraphEndNodeName(name string) bool {
	return name == EndNode
}

func isGraphTerminalEdge(edge EdgeSnapshot) bool {
	return edge.From == StartNode || edge.To == EndNode
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
