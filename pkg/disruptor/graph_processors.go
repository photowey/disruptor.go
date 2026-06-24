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
	"sort"
	"sync"
)

// GraphHandleOption configures graph processor registration.
type GraphHandleOption[T any] interface {
	applyGraphHandle(config *graphHandleConfig[T]) error
}

type graphHandleConfig[T any] struct {
	exceptionHandler ExceptionHandler[T]
}

type graphHandleOptionFunc[T any] struct {
	applyFunc func(*graphHandleConfig[T]) error
}

//nolint:unused // The method satisfies GraphHandleOption[T] and is called through the interface.
func (fn graphHandleOptionFunc[T]) applyGraphHandle(config *graphHandleConfig[T]) error {
	return fn.applyFunc(config)
}

// WithGraphExceptionHandler sets the default exception handler for graph nodes.
func WithGraphExceptionHandler[T any](handler ExceptionHandler[T]) GraphHandleOption[T] {
	return graphHandleOptionFunc[T]{
		applyFunc: func(config *graphHandleConfig[T]) error {
			if handler == nil {
				return fmt.Errorf("%w: graph exception handler is nil", ErrInvalidGraph)
			}

			config.exceptionHandler = handler
			return nil
		},
	}
}

// GraphProcessors exposes processors created from a handled graph.
type GraphProcessors interface {
	Names() []string
	Processors() []EventProcessor
	Processor(name string) (EventProcessor, bool)
	Sequence(name string) (*Sequence, bool)
	Snapshot() GraphSnapshot
}

type handledGraphProcessors struct {
	names      []string
	processors map[string]EventProcessor
	snapshot   GraphSnapshot
}

// HandleGraph registers a named dependency graph of event handlers.
func (d *Disruptor[T]) HandleGraph(
	graph *Graph[T],
	opts ...GraphHandleOption[T],
) (GraphProcessors, error) {
	if graph == nil {
		return nil, fmt.Errorf("%w: graph is nil", ErrInvalidGraph)
	}

	handleConfig := graphHandleConfig[T]{
		exceptionHandler: defaultProcessorConfig[T]().exceptionHandler,
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt.applyGraphHandle(&handleConfig); err != nil {
			return nil, fmt.Errorf("applying graph handle option: %w", err)
		}
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if d.started.Load() {
		return nil, ErrClosed
	}
	if d.mode == consumerModeFanOut {
		return nil, fmt.Errorf(
			"%w: HandleGraph cannot be used after HandleEventsWith",
			ErrConsumerModeConflict,
		)
	}

	graph.mu.Lock()
	defer graph.mu.Unlock()
	if graph.handled {
		return nil, ErrGraphHandled
	}
	if err := graph.validateLocked(); err != nil {
		return nil, err
	}

	processingSnapshot := graph.processingSnapshotLocked()
	order := graphTopologicalOrder(processingSnapshot)
	upstream := graphUpstream(processingSnapshot.Edges)
	leafSet := graphNameSet(processingSnapshot.Leaves)
	nodeByName := make(map[string]*graphNode[T], len(graph.nodes))
	for name, node := range graph.nodes {
		nodeByName[name] = node
	}

	var stopOnce sync.Once
	createdProcessors := make([]EventProcessor, 0, len(order))
	stopGraph := func() {
		stopOnce.Do(func() {
			for _, processor := range createdProcessors {
				processor.Stop()
			}
		})
	}

	processorByName := make(map[string]EventProcessor, len(order))
	for _, name := range order {
		node := nodeByName[name]
		dependencies := make([]*Sequence, 0, len(upstream[name]))
		for _, upstreamName := range upstream[name] {
			processor, ok := processorByName[upstreamName]
			if !ok {
				return nil, fmt.Errorf(
					"%w: graph %s: missing upstream processor %s",
					ErrInvalidGraph,
					graph.name,
					upstreamName,
				)
			}
			dependencies = append(dependencies, processor.Sequence())
		}

		exceptionHandler := node.exceptionHandler
		if exceptionHandler == nil {
			exceptionHandler = handleConfig.exceptionHandler
		}

		processor, err := newBatchEventProcessor(
			d.ringBuffer,
			d.ringBuffer.NewBarrier(dependencies...),
			node.handler,
			batchEventProcessorConfig[T]{
				exceptionHandler: exceptionHandler,
				producerGating:   leafSet[name],
				haltAdvances:     false,
				node: NodeContext{
					GraphName: graph.name,
					NodeName:  node.name,
					NodeLabel: node.label,
				},
				onHalt: stopGraph,
			},
		)
		if err != nil {
			return nil, fmt.Errorf("creating graph processor %s: %w", name, err)
		}

		processorByName[name] = processor
		createdProcessors = append(createdProcessors, processor)
	}

	graph.freezeHandledLocked()
	publicSnapshot := graph.snapshotLocked()
	d.mode = consumerModeGraph
	d.processors = append(d.processors, createdProcessors...)

	return newHandledGraphProcessors(publicSnapshot, processorByName), nil
}

func newHandledGraphProcessors(
	snapshot GraphSnapshot,
	processors map[string]EventProcessor,
) *handledGraphProcessors {
	names := make([]string, 0, len(processors))
	copiedProcessors := make(map[string]EventProcessor, len(processors))
	for name, processor := range processors {
		names = append(names, name)
		copiedProcessors[name] = processor
	}
	sort.Strings(names)

	return &handledGraphProcessors{
		names:      names,
		processors: copiedProcessors,
		snapshot:   copyGraphSnapshot(snapshot),
	}
}

func (p *handledGraphProcessors) Names() []string {
	return append([]string(nil), p.names...)
}

func (p *handledGraphProcessors) Processors() []EventProcessor {
	processors := make([]EventProcessor, 0, len(p.names))
	for _, name := range p.names {
		processors = append(processors, p.processors[name])
	}

	return processors
}

func (p *handledGraphProcessors) Processor(name string) (EventProcessor, bool) {
	processor, ok := p.processors[name]
	return processor, ok
}

func (p *handledGraphProcessors) Sequence(name string) (*Sequence, bool) {
	processor, ok := p.processors[name]
	if !ok {
		return nil, false
	}

	return processor.Sequence(), true
}

func (p *handledGraphProcessors) Snapshot() GraphSnapshot {
	return copyGraphSnapshot(p.snapshot)
}

func graphTopologicalOrder(snapshot GraphSnapshot) []string {
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

func graphUpstream(edges []GraphEdgeSnapshot) map[string][]string {
	upstream := make(map[string][]string)
	for _, edge := range edges {
		upstream[edge.To] = append(upstream[edge.To], edge.From)
	}
	for name := range upstream {
		sort.Strings(upstream[name])
	}

	return upstream
}

func graphNameSet(names []string) map[string]bool {
	set := make(map[string]bool, len(names))
	for _, name := range names {
		set[name] = true
	}

	return set
}
