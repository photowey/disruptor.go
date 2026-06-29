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
	"github.com/photowey/disruptor.go/pkg/processor"
	"github.com/photowey/disruptor.go/pkg/sequence"
)

// GraphOption configures graph processor registration.
type GraphOption[T any] func(options *graphOptions[T]) error

type graphOptions[T any] struct {
	exceptionHandler event.ExceptionHandler[T]
}

// WithGraphExceptionHandler sets the default exception handler for graph nodes.
func WithGraphExceptionHandler[T any](handler event.ExceptionHandler[T]) GraphOption[T] {
	return func(options *graphOptions[T]) error {
		if handler == nil {
			return fmt.Errorf("%w: graph exception handler is nil", topology.ErrInvalid)
		}

		options.exceptionHandler = handler
		return nil
	}
}

// GraphProcessors exposes processors created from a handled graph.
type GraphProcessors interface {
	Names() []string
	Processors() []processor.EventProcessor
	Processor(name string) (processor.EventProcessor, bool)
	Sequence(name string) (*sequence.Sequence, bool)
	Snapshot() topology.Snapshot
}

type handledGraphProcessors struct {
	names      []string
	processors map[string]processor.EventProcessor
	snapshot   topology.Snapshot
}

// HandleGraph registers a named dependency graph of event handlers.
func (d *Disruptor[T]) HandleGraph(
	topologyGraph *topology.Graph[T],
	opts ...GraphOption[T],
) (GraphProcessors, error) {
	if topologyGraph == nil {
		return nil, fmt.Errorf("%w: graph is nil", topology.ErrInvalid)
	}

	options := graphOptions[T]{
		exceptionHandler: event.NewFatalExceptionHandler[T](),
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(&options); err != nil {
			return nil, fmt.Errorf("applying graph option: %w", err)
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

	createdProcessors := make([]processor.EventProcessor, 0, len(plan.Order))
	haltNotifier := &graphProcessorHaltNotifier{
		processors: &createdProcessors,
	}

	processorByName := make(map[string]processor.EventProcessor, len(plan.Order))
	for _, name := range plan.Order {
		node := plan.Nodes[name]
		dependencies := make([]*sequence.Sequence, 0, len(plan.Upstream[name]))
		for _, upstreamName := range plan.Upstream[name] {
			eventProcessor, ok := processorByName[upstreamName]
			if !ok {
				return nil, fmt.Errorf(
					"%w: graph %s: missing upstream processor %s",
					topology.ErrInvalid,
					plan.Name,
					upstreamName,
				)
			}
			dependencies = append(dependencies, eventProcessor.Sequence())
		}

		exceptionHandler := node.ExceptionHandler
		if exceptionHandler == nil {
			exceptionHandler = options.exceptionHandler
		}

		eventProcessor, err := processor.NewBatchEventProcessorWithConfig(
			d.ringBuffer,
			d.ringBuffer.NewBarrier(dependencies...),
			node.Handler,
			processor.BatchConfig[T]{
				ExceptionHandler: exceptionHandler,
				ProducerGating:   plan.Leaves[name],
				HaltAdvances:     false,
				Node: event.Node{
					GraphName: plan.Name,
					NodeName:  node.Name,
					NodeLabel: node.Label,
				},
				HaltNotifier: haltNotifier,
			},
		)
		if err != nil {
			return nil, fmt.Errorf("creating graph processor %s: %w", name, err)
		}

		processorByName[name] = eventProcessor
		createdProcessors = append(createdProcessors, eventProcessor)
	}

	d.mode = consumerModeGraph
	d.processors = append(d.processors, createdProcessors...)

	return newHandledGraphProcessors(plan.Snapshot, processorByName), nil
}

type graphProcessorHaltNotifier struct {
	once       sync.Once
	processors *[]processor.EventProcessor
}

func (notifier *graphProcessorHaltNotifier) NotifyHalt() {
	notifier.once.Do(notifier.stopProcessors)
}

func (notifier *graphProcessorHaltNotifier) stopProcessors() {
	for _, processor := range *notifier.processors {
		processor.Stop()
	}
}

func newHandledGraphProcessors(
	snapshot topology.Snapshot,
	processors map[string]processor.EventProcessor,
) *handledGraphProcessors {
	names := make([]string, 0, len(processors))
	copiedProcessors := make(map[string]processor.EventProcessor, len(processors))
	for name, eventProcessor := range processors {
		names = append(names, name)
		copiedProcessors[name] = eventProcessor
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

func (p *handledGraphProcessors) Processors() []processor.EventProcessor {
	processors := make([]processor.EventProcessor, 0, len(p.names))
	for _, name := range p.names {
		processors = append(processors, p.processors[name])
	}

	return processors
}

func (p *handledGraphProcessors) Processor(name string) (processor.EventProcessor, bool) {
	eventProcessor, ok := p.processors[name]
	return eventProcessor, ok
}

func (p *handledGraphProcessors) Sequence(name string) (*sequence.Sequence, bool) {
	eventProcessor, ok := p.processors[name]
	if !ok {
		return nil, false
	}

	return eventProcessor.Sequence(), true
}

func (p *handledGraphProcessors) Snapshot() topology.Snapshot {
	return p.snapshot.Copy()
}
