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

	"github.com/photowey/disruptor.go/pkg/event"
	topology "github.com/photowey/disruptor.go/pkg/graph"
)

// GraphHandleOption configures graph processor registration.
type GraphHandleOption[T any] interface {
	applyGraphHandle(config *graphHandleConfig[T]) error
}

type graphHandleConfig[T any] struct {
	exceptionHandler event.ExceptionHandler[T]
}

type graphHandleOptionFunc[T any] struct {
	applyFunc func(*graphHandleConfig[T]) error
}

//nolint:unused // The method satisfies GraphHandleOption[T] and is called through the interface.
func (fn graphHandleOptionFunc[T]) applyGraphHandle(config *graphHandleConfig[T]) error {
	return fn.applyFunc(config)
}

// WithGraphExceptionHandler sets the default exception handler for graph nodes.
func WithGraphExceptionHandler[T any](handler event.ExceptionHandler[T]) GraphHandleOption[T] {
	return graphHandleOptionFunc[T]{
		applyFunc: func(config *graphHandleConfig[T]) error {
			if handler == nil {
				return fmt.Errorf("%w: graph exception handler is nil", topology.ErrInvalid)
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
	Snapshot() topology.Snapshot
}

type handledGraphProcessors struct {
	names      []string
	processors map[string]EventProcessor
	snapshot   topology.Snapshot
}

// HandleGraph registers a named dependency graph of event handlers.
func (d *Disruptor[T]) HandleGraph(
	topologyGraph *topology.Graph[T],
	opts ...GraphHandleOption[T],
) (GraphProcessors, error) {
	if topologyGraph == nil {
		return nil, fmt.Errorf("%w: graph is nil", topology.ErrInvalid)
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

	plan, err := topologyGraph.BuildPlan()
	if err != nil {
		return nil, err
	}

	var stopOnce sync.Once
	createdProcessors := make([]EventProcessor, 0, len(plan.Order))
	stopGraph := func() {
		stopOnce.Do(func() {
			for _, processor := range createdProcessors {
				processor.Stop()
			}
		})
	}

	processorByName := make(map[string]EventProcessor, len(plan.Order))
	for _, name := range plan.Order {
		node := plan.Nodes[name]
		dependencies := make([]*Sequence, 0, len(plan.Upstream[name]))
		for _, upstreamName := range plan.Upstream[name] {
			processor, ok := processorByName[upstreamName]
			if !ok {
				return nil, fmt.Errorf(
					"%w: graph %s: missing upstream processor %s",
					topology.ErrInvalid,
					plan.Name,
					upstreamName,
				)
			}
			dependencies = append(dependencies, processor.Sequence())
		}

		exceptionHandler := node.ExceptionHandler
		if exceptionHandler == nil {
			exceptionHandler = handleConfig.exceptionHandler
		}

		processor, err := newBatchEventProcessor(
			d.ringBuffer,
			d.ringBuffer.NewBarrier(dependencies...),
			node.Handler,
			batchEventProcessorConfig[T]{
				exceptionHandler: exceptionHandler,
				producerGating:   plan.Leaves[name],
				haltAdvances:     false,
				node: event.Node{
					GraphName: plan.Name,
					NodeName:  node.Name,
					NodeLabel: node.Label,
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

	d.mode = consumerModeGraph
	d.processors = append(d.processors, createdProcessors...)

	return newHandledGraphProcessors(plan.Snapshot, processorByName), nil
}

func newHandledGraphProcessors(
	snapshot topology.Snapshot,
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
		snapshot:   snapshot.Copy(),
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

func (p *handledGraphProcessors) Snapshot() topology.Snapshot {
	return p.snapshot.Copy()
}
