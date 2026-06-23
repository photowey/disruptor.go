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
	"fmt"
	"strings"
	"sync"
	"unicode"
)

// Graph defines a named event processor dependency topology.
type Graph[T any] struct {
	mu sync.RWMutex

	name    string
	nodes   map[string]*graphNode[T]
	edges   map[GraphEdgeSnapshot]struct{}
	frozen  bool
	handled bool
}

type graphNode[T any] struct {
	name             string
	handler          EventHandler[T]
	exceptionHandler ExceptionHandler[T]
	label            string
	metadata         map[string]string
}

type nodeConfig[T any] struct {
	exceptionHandler ExceptionHandler[T]
	label            string
	metadata         map[string]string
}

// NodeOption configures one graph node.
type NodeOption[T any] interface {
	applyNode(config *nodeConfig[T]) error
}

type nodeOptionFunc[T any] struct {
	applyFunc func(*nodeConfig[T]) error
}

//nolint:unused // The method satisfies NodeOption[T] and is called through the interface.
func (fn nodeOptionFunc[T]) applyNode(config *nodeConfig[T]) error {
	return fn.applyFunc(config)
}

// NewGraph creates a mutable graph builder.
func NewGraph[T any](name string) (*Graph[T], error) {
	normalized, err := normalizeGraphName("graph", name)
	if err != nil {
		return nil, err
	}

	return &Graph[T]{
		name:  normalized,
		nodes: make(map[string]*graphNode[T]),
		edges: make(map[GraphEdgeSnapshot]struct{}),
	}, nil
}

// MustGraph creates a graph builder or panics when name is invalid.
func MustGraph[T any](name string) *Graph[T] {
	graph, err := NewGraph[T](name)
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
	handler EventHandler[T],
	opts ...NodeOption[T],
) error {
	if g == nil {
		return fmt.Errorf("%w: graph is nil", ErrInvalidGraph)
	}
	if handler == nil {
		return fmt.Errorf("%w: graph %s: node handler is nil", ErrInvalidGraph, g.Name())
	}

	normalized, err := normalizeGraphName("node", name)
	if err != nil {
		return err
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
			return fmt.Errorf("applying node option: %w", err)
		}
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if g.frozen {
		return ErrGraphFrozen
	}
	if _, exists := g.nodes[normalized]; exists {
		return fmt.Errorf(
			"%w: graph %s: node %s already exists",
			ErrInvalidGraph,
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
	handler EventHandler[T],
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
		return fmt.Errorf("%w: graph is nil", ErrInvalidGraph)
	}

	normalizedFrom, err := normalizeGraphName("node", from)
	if err != nil {
		return err
	}
	normalizedTo, err := normalizeGraphName("node", to)
	if err != nil {
		return err
	}
	if normalizedFrom == normalizedTo {
		return fmt.Errorf(
			"%w: graph %s: self edge %s -> %s",
			ErrInvalidGraph,
			g.Name(),
			normalizedFrom,
			normalizedTo,
		)
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if g.frozen {
		return ErrGraphFrozen
	}
	if _, exists := g.nodes[normalizedFrom]; !exists {
		return fmt.Errorf(
			"%w: graph %s: edge %s -> %s references unknown node %s",
			ErrInvalidGraph,
			g.name,
			normalizedFrom,
			normalizedTo,
			normalizedFrom,
		)
	}
	if _, exists := g.nodes[normalizedTo]; !exists {
		return fmt.Errorf(
			"%w: graph %s: edge %s -> %s references unknown node %s",
			ErrInvalidGraph,
			g.name,
			normalizedFrom,
			normalizedTo,
			normalizedTo,
		)
	}

	g.edges[GraphEdgeSnapshot{From: normalizedFrom, To: normalizedTo}] = struct{}{}

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
		return fmt.Errorf("%w: graph is nil", ErrInvalidGraph)
	}

	g.mu.RLock()
	defer g.mu.RUnlock()

	return g.validateLocked()
}

func (g *Graph[T]) validateLocked() error {
	if len(g.nodes) == 0 {
		return fmt.Errorf("%w: graph %s: no nodes", ErrInvalidGraph, g.name)
	}

	if len(g.nodes) > 1 {
		snapshot := g.snapshotLocked()
		connected := make(map[string]struct{}, len(g.nodes))
		for _, edge := range snapshot.Edges {
			connected[edge.From] = struct{}{}
			connected[edge.To] = struct{}{}
		}
		for _, node := range snapshot.Nodes {
			if _, ok := connected[node.Name]; !ok {
				return fmt.Errorf(
					"%w: graph %s: node %s is isolated",
					ErrInvalidGraph,
					g.name,
					node.Name,
				)
			}
		}
	}

	if cycle := g.findCycleLocked(); len(cycle) > 0 {
		return fmt.Errorf(
			"%w: graph %s: cycle detected: %s",
			ErrInvalidGraph,
			g.name,
			strings.Join(cycle, " -> "),
		)
	}

	return nil
}

func (g *Graph[T]) freezeHandledLocked() {
	g.frozen = true
	g.handled = true
}

// WithNodeExceptionHandler sets the exception handler for one graph node.
func WithNodeExceptionHandler[T any](handler ExceptionHandler[T]) NodeOption[T] {
	return nodeOptionFunc[T]{
		applyFunc: func(config *nodeConfig[T]) error {
			if handler == nil {
				return fmt.Errorf("%w: node exception handler is nil", ErrInvalidGraph)
			}

			config.exceptionHandler = handler
			return nil
		},
	}
}

// WithNodeLabel sets the display label for one graph node.
func WithNodeLabel[T any](label string) NodeOption[T] {
	return nodeOptionFunc[T]{
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

// WithNodeMetadata adds one metadata key-value pair to a graph node.
func WithNodeMetadata[T any](key string, value string) NodeOption[T] {
	return nodeOptionFunc[T]{
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

func normalizeGraphName(kind string, name string) (string, error) {
	normalized := strings.TrimSpace(name)
	if normalized == "" {
		return "", fmt.Errorf("%w: %s name is empty", ErrInvalidGraph, kind)
	}
	for _, ch := range normalized {
		if unicode.IsControl(ch) {
			return "", fmt.Errorf(
				"%w: %s name %q contains a control character",
				ErrInvalidGraph,
				kind,
				normalized,
			)
		}
	}

	return normalized, nil
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
