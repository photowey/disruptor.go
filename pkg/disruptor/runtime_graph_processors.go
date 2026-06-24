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
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/photowey/disruptor.go/pkg/event"
	"github.com/photowey/disruptor.go/pkg/graph"
	topology "github.com/photowey/disruptor.go/pkg/runtimegraph"
	"github.com/photowey/disruptor.go/pkg/runtimevars"
)

// RuntimeGraphExceptionKind classifies runtime graph failures.
type RuntimeGraphExceptionKind uint8

const (
	// RuntimeGraphExceptionKindUnknown is the zero value.
	RuntimeGraphExceptionKindUnknown RuntimeGraphExceptionKind = iota
	// RuntimeGraphExceptionKindHandler reports a handler failure.
	RuntimeGraphExceptionKindHandler
	// RuntimeGraphExceptionKindCondition reports a condition failure.
	RuntimeGraphExceptionKindCondition
	// RuntimeGraphExceptionKindNoRoute reports a no-route outcome.
	RuntimeGraphExceptionKindNoRoute
	// RuntimeGraphExceptionKindPanic reports a recovered panic.
	RuntimeGraphExceptionKindPanic
)

// RuntimeGraphExceptionRequest describes a runtime graph failure.
type RuntimeGraphExceptionRequest[T any] struct {
	Context   context.Context
	Event     *T
	Sequence  int64
	GraphName string
	NodeName  string
	EdgeFrom  string
	EdgeTo    string
	Kind      RuntimeGraphExceptionKind
	Cause     error
	Runtime   runtimevars.ContextView
}

// RuntimeGraphExceptionHandler decides how runtime graph failures are handled.
type RuntimeGraphExceptionHandler[T any] interface {
	HandleRuntimeGraphException(request RuntimeGraphExceptionRequest[T]) event.ExceptionAction
}

// RuntimeGraphExceptionHandlerFunc adapts a function to RuntimeGraphExceptionHandler.
type RuntimeGraphExceptionHandlerFunc[T any] func(
	request RuntimeGraphExceptionRequest[T],
) event.ExceptionAction

// HandleRuntimeGraphException calls the wrapped function.
func (fn RuntimeGraphExceptionHandlerFunc[T]) HandleRuntimeGraphException(
	request RuntimeGraphExceptionRequest[T],
) event.ExceptionAction {
	if fn == nil {
		return event.ExceptionActionHalt
	}

	return fn(request)
}

// RuntimeNoRouteAction determines how a no-route runtime graph outcome is handled.
type RuntimeNoRouteAction uint8

const (
	// RuntimeNoRouteActionHalt stops the processor and reports runtimegraph.ErrNoRoute.
	RuntimeNoRouteActionHalt RuntimeNoRouteAction = iota
	// RuntimeNoRouteActionComplete completes the event without error.
	RuntimeNoRouteActionComplete
)

// RuntimeGraphHandleOption configures runtime graph registration.
type RuntimeGraphHandleOption[T any] interface {
	applyRuntimeGraphHandle(config *runtimeGraphHandleConfig[T]) error
}

type runtimeGraphHandleConfig[T any] struct {
	exceptionHandler RuntimeGraphExceptionHandler[T]
	noRouteAction    RuntimeNoRouteAction
	workers          int
	provider         runtimevars.Provider[T]
	resolver         runtimevars.Resolver[T]
	metricsSink      RuntimeGraphMetricsSink
}

type runtimeGraphHandleOptionFunc[T any] struct {
	applyFunc func(*runtimeGraphHandleConfig[T]) error
}

//nolint:unused // The method satisfies RuntimeGraphHandleOption[T] and is called through the interface.
func (fn runtimeGraphHandleOptionFunc[T]) applyRuntimeGraphHandle(
	config *runtimeGraphHandleConfig[T],
) error {
	return fn.applyFunc(config)
}

// WithRuntimeGraphExceptionHandler sets the runtime graph failure handler.
func WithRuntimeGraphExceptionHandler[T any](
	handler RuntimeGraphExceptionHandler[T],
) RuntimeGraphHandleOption[T] {
	return runtimeGraphHandleOptionFunc[T]{
		applyFunc: func(config *runtimeGraphHandleConfig[T]) error {
			if handler == nil {
				return fmt.Errorf("%w: runtime graph exception handler is nil", topology.ErrInvalid)
			}

			config.exceptionHandler = handler
			return nil
		},
	}
}

// WithRuntimeGraphWorkers configures the runtime graph worker count.
func WithRuntimeGraphWorkers[T any](workers int) RuntimeGraphHandleOption[T] {
	return runtimeGraphHandleOptionFunc[T]{
		applyFunc: func(config *runtimeGraphHandleConfig[T]) error {
			if workers < 1 {
				return fmt.Errorf("%w: runtime graph workers must be positive", topology.ErrInvalid)
			}

			config.workers = workers
			return nil
		},
	}
}

// WithRuntimeGraphNoRouteAction configures the runtime graph no-route action.
func WithRuntimeGraphNoRouteAction[T any](
	action RuntimeNoRouteAction,
) RuntimeGraphHandleOption[T] {
	return runtimeGraphHandleOptionFunc[T]{
		applyFunc: func(config *runtimeGraphHandleConfig[T]) error {
			switch action {
			case RuntimeNoRouteActionHalt, RuntimeNoRouteActionComplete:
				config.noRouteAction = action
				return nil
			default:
				return fmt.Errorf("%w: invalid runtime graph no-route action", topology.ErrInvalid)
			}
		},
	}
}

// WithRuntimeGraphVariablesProvider sets a runtime variables provider.
func WithRuntimeGraphVariablesProvider[T any](
	provider runtimevars.Provider[T],
) RuntimeGraphHandleOption[T] {
	return runtimeGraphHandleOptionFunc[T]{
		applyFunc: func(config *runtimeGraphHandleConfig[T]) error {
			config.provider = provider
			return nil
		},
	}
}

// WithRuntimeGraphEventValueResolver sets the event value resolver.
func WithRuntimeGraphEventValueResolver[T any](
	resolver runtimevars.Resolver[T],
) RuntimeGraphHandleOption[T] {
	return runtimeGraphHandleOptionFunc[T]{
		applyFunc: func(config *runtimeGraphHandleConfig[T]) error {
			config.resolver = resolver
			return nil
		},
	}
}

// WithRuntimeGraphMetricsSink sets the runtime graph metrics sink.
func WithRuntimeGraphMetricsSink[T any](
	sink RuntimeGraphMetricsSink,
) RuntimeGraphHandleOption[T] {
	return runtimeGraphHandleOptionFunc[T]{
		applyFunc: func(config *runtimeGraphHandleConfig[T]) error {
			config.metricsSink = sink
			return nil
		},
	}
}

// RuntimeGraphMetricsSink receives optional runtime graph metrics.
type RuntimeGraphMetricsSink interface {
	OnRuntimeGraph(request RuntimeGraphMetric)
}

// RuntimeGraphMetric describes a runtime graph telemetry event.
type RuntimeGraphMetric struct {
	Kind      string
	GraphName string
	Node      event.Node
	EdgeFrom  string
	EdgeTo    string
	Sequence  int64
	Duration  time.Duration
	Err       error
	Selected  bool
}

// RuntimeGraphProcessors exposes the processors created from a handled runtime graph.
type RuntimeGraphProcessors interface {
	Processor() EventProcessor
	Sequence() *Sequence
	Snapshot() topology.RuntimeGraphSnapshot
}

type handledRuntimeGraphProcessors struct {
	processor EventProcessor
	snapshot  topology.RuntimeGraphSnapshot
}

func (p *handledRuntimeGraphProcessors) Processor() EventProcessor {
	return p.processor
}

func (p *handledRuntimeGraphProcessors) Sequence() *Sequence {
	if p == nil || p.processor == nil {
		return nil
	}

	return p.processor.Sequence()
}

func (p *handledRuntimeGraphProcessors) Snapshot() topology.RuntimeGraphSnapshot {
	return p.snapshot.Copy()
}

// HandleRuntimeGraph registers a runtime graph scheduler.
func (d *Disruptor[T]) HandleRuntimeGraph(
	runtimeGraph *topology.RuntimeGraph[T],
	opts ...RuntimeGraphHandleOption[T],
) (RuntimeGraphProcessors, error) {
	if runtimeGraph == nil {
		return nil, fmt.Errorf("%w: runtime graph is nil", topology.ErrInvalid)
	}

	handleConfig := runtimeGraphHandleConfig[T]{
		exceptionHandler: NewFatalRuntimeGraphExceptionHandler[T](),
		noRouteAction:    RuntimeNoRouteActionHalt,
		workers:          1,
		resolver:         runtimevars.NewReflectionResolver[T](),
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt.applyRuntimeGraphHandle(&handleConfig); err != nil {
			return nil, fmt.Errorf("applying runtime graph handle option: %w", err)
		}
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if d.started.Load() {
		return nil, ErrClosed
	}
	if d.mode == consumerModeFanOut {
		return nil, fmt.Errorf(
			"%w: HandleRuntimeGraph cannot be used after HandleEventsWith",
			ErrConsumerModeConflict,
		)
	}

	plan, err := runtimeGraph.BuildPlan()
	if err != nil {
		return nil, err
	}

	if handleConfig.metricsSink == nil {
		if metricsSink, ok := d.ringBuffer.metrics.(RuntimeGraphMetricsSink); ok {
			handleConfig.metricsSink = metricsSink
		}
	}

	handler := &runtimeGraphEventHandler[T]{
		graphName:        plan.Name,
		plan:             plan,
		exceptionHandler: handleConfig.exceptionHandler,
		noRouteAction:    handleConfig.noRouteAction,
		provider:         handleConfig.provider,
		resolver:         handleConfig.resolver,
		metricsSink:      handleConfig.metricsSink,
		workers:          handleConfig.workers,
	}

	stopOnce := &sync.Once{}
	var processor *BatchEventProcessor[T]
	stopGraph := func() {
		stopOnce.Do(func() {
			if processor != nil {
				processor.Stop()
			}
		})
	}

	processor, err = newBatchEventProcessor(
		d.ringBuffer,
		d.ringBuffer.NewBarrier(),
		handler,
		batchEventProcessorConfig[T]{
			exceptionHandler: defaultProcessorConfig[T]().exceptionHandler,
			producerGating:   true,
			haltAdvances:     false,
			node: event.Node{
				GraphName: plan.Name,
				NodeName:  "scheduler",
				NodeLabel: plan.Name,
			},
			onHalt: stopGraph,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("creating runtime graph processor: %w", err)
	}

	d.mode = consumerModeGraph
	d.processors = append(d.processors, processor)

	return &handledRuntimeGraphProcessors{
		processor: processor,
		snapshot:  plan.Snapshot,
	}, nil
}

type runtimeGraphEventHandler[T any] struct {
	graphName        string
	plan             *topology.Plan[T]
	exceptionHandler RuntimeGraphExceptionHandler[T]
	noRouteAction    RuntimeNoRouteAction
	provider         runtimevars.Provider[T]
	resolver         runtimevars.Resolver[T]
	metricsSink      RuntimeGraphMetricsSink
	workers          int
}

func (h *runtimeGraphEventHandler[T]) OnStart(ctx context.Context) error {
	if h.workers < 1 {
		return fmt.Errorf("%w: runtime graph workers must be positive", topology.ErrInvalid)
	}

	return nil
}

func (h *runtimeGraphEventHandler[T]) OnShutdown(ctx context.Context) error {
	return nil
}

func (h *runtimeGraphEventHandler[T]) OnEvent(request event.Request[T]) error {
	var providerVariables runtimevars.Variables
	if h.provider != nil {
		var err error
		providerVariables, err = h.provider.Variables(runtimevars.ProviderRequest[T]{
			Context:   request.Context,
			Event:     request.Event,
			Sequence:  request.Sequence,
			GraphName: h.graphName,
		})
		if err != nil {
			return err
		}
	}

	runtimeCtx := runtimevars.NewContext(
		runtimevars.Request[T]{
			Context:  request.Context,
			Event:    request.Event,
			Sequence: request.Sequence,
		},
		h.graphName,
		providerVariables,
		h.resolver,
	)
	state := newRuntimeGraphRunState[T](h.plan, runtimeCtx, request)
	if err := state.processStart(h); err != nil {
		return err
	}
	for len(state.ready) > 0 {
		name := state.popReady()
		if err := state.runNode(h, name); err != nil {
			return err
		}
	}

	if state.endReached {
		h.emitMetric(RuntimeGraphMetric{
			Kind:      "complete",
			GraphName: h.graphName,
			Sequence:  request.Sequence,
		})
		return nil
	}

	switch h.noRouteAction {
	case RuntimeNoRouteActionComplete:
		h.emitMetric(RuntimeGraphMetric{
			Kind:      "no_route_complete",
			GraphName: h.graphName,
			Sequence:  request.Sequence,
		})
		return nil
	default:
		return h.raiseRuntimeException(RuntimeGraphExceptionRequest[T]{
			Context:   request.Context,
			Event:     request.Event,
			Sequence:  request.Sequence,
			GraphName: h.graphName,
			Kind:      RuntimeGraphExceptionKindNoRoute,
			Cause:     topology.ErrNoRoute,
			Runtime:   runtimeCtx,
		})
	}
}

func (h *runtimeGraphEventHandler[T]) emitMetric(metric RuntimeGraphMetric) {
	if h.metricsSink == nil {
		return
	}

	h.metricsSink.OnRuntimeGraph(metric)
}

func (h *runtimeGraphEventHandler[T]) handleRuntimeException(
	request RuntimeGraphExceptionRequest[T],
) event.ExceptionAction {
	h.emitMetric(RuntimeGraphMetric{
		Kind:      "exception",
		GraphName: request.GraphName,
		Node: event.Node{
			GraphName: request.GraphName,
			NodeName:  request.NodeName,
		},
		EdgeFrom: request.EdgeFrom,
		EdgeTo:   request.EdgeTo,
		Sequence: request.Sequence,
		Err:      request.Cause,
	})
	if h.exceptionHandler == nil {
		return event.ExceptionActionHalt
	}

	action := h.exceptionHandler.HandleRuntimeGraphException(request)
	if action == event.ExceptionActionUnknown {
		return event.ExceptionActionHalt
	}

	return action
}

func (h *runtimeGraphEventHandler[T]) raiseRuntimeException(
	request RuntimeGraphExceptionRequest[T],
) error {
	if h.handleRuntimeException(request) == event.ExceptionActionContinue {
		return nil
	}

	return request.Cause
}

type runtimeGraphRunState[T any] struct {
	plan       *topology.Plan[T]
	runtime    runtimevars.ContextView
	request    event.Request[T]
	nodes      map[string]*runtimeGraphNodeState
	ready      []string
	endReached bool
}

type runtimeGraphNodeState struct {
	total     int
	resolved  int
	selected  int
	scheduled bool
	done      bool
}

func newRuntimeGraphRunState[T any](
	plan *topology.Plan[T],
	runtime runtimevars.ContextView,
	request event.Request[T],
) *runtimeGraphRunState[T] {
	nodes := make(map[string]*runtimeGraphNodeState, len(plan.Nodes))
	for name, node := range plan.Nodes {
		nodes[name] = &runtimeGraphNodeState{total: node.Incoming}
	}

	return &runtimeGraphRunState[T]{
		plan:    plan,
		runtime: runtime,
		request: request,
		nodes:   nodes,
		ready:   make([]string, 0, len(plan.Nodes)),
	}
}

func (s *runtimeGraphRunState[T]) processStart(handler *runtimeGraphEventHandler[T]) error {
	for _, edge := range s.plan.Start {
		selected, err := edge.Evaluate(topology.EdgeConditionRequest[T]{
			Context:   s.request.Context,
			Event:     s.request.Event,
			Sequence:  s.request.Sequence,
			GraphName: handler.graphName,
			From:      edge.From,
			To:        edge.To,
			Runtime:   s.runtime,
		})
		if err != nil {
			action := handler.handleRuntimeException(RuntimeGraphExceptionRequest[T]{
				Context:   s.request.Context,
				Event:     s.request.Event,
				Sequence:  s.request.Sequence,
				GraphName: handler.graphName,
				EdgeFrom:  edge.From,
				EdgeTo:    edge.To,
				Kind:      RuntimeGraphExceptionKindCondition,
				Cause:     err,
				Runtime:   s.runtime,
			})
			if action != event.ExceptionActionContinue {
				return err
			}
			selected = false
		}
		handler.emitMetric(RuntimeGraphMetric{
			Kind:      runtimeGraphEdgeMetricKind(selected),
			GraphName: handler.graphName,
			EdgeFrom:  edge.From,
			EdgeTo:    edge.To,
			Sequence:  s.request.Sequence,
			Selected:  selected,
		})
		if edge.To == graph.EndNode {
			if selected {
				s.endReached = true
			}
			continue
		}
		if err := s.resolveInbound(handler, edge.To, selected); err != nil {
			return err
		}
	}

	return nil
}

func (s *runtimeGraphRunState[T]) popReady() string {
	name := s.ready[0]
	s.ready = s.ready[1:]
	return name
}

func (s *runtimeGraphRunState[T]) resolveInbound(
	handler *runtimeGraphEventHandler[T],
	name string,
	selected bool,
) error {
	node := s.nodes[name]
	if node == nil || node.done {
		return nil
	}

	node.resolved++
	if selected {
		node.selected++
	}
	if node.resolved < node.total || node.scheduled {
		return nil
	}
	node.scheduled = true
	if node.selected == 0 {
		node.done = true
		handler.emitMetric(RuntimeGraphMetric{
			Kind:      "node_skipped",
			GraphName: handler.graphName,
			Node: event.Node{
				GraphName: handler.graphName,
				NodeName:  name,
			},
			Sequence: s.request.Sequence,
		})
		for _, edge := range s.plan.Nodes[name].Outgoing {
			if edge.To == graph.EndNode {
				continue
			}
			if err := s.resolveInbound(handler, edge.To, false); err != nil {
				return err
			}
		}

		return nil
	}

	s.ready = append(s.ready, name)
	sort.Strings(s.ready)

	return nil
}

func (s *runtimeGraphRunState[T]) runNode(
	handler *runtimeGraphEventHandler[T],
	name string,
) error {
	nodeState := s.nodes[name]
	if nodeState == nil || nodeState.done {
		return nil
	}

	planNode := s.plan.Nodes[name]
	request := event.Request[T]{
		Context:    s.request.Context,
		Event:      s.request.Event,
		Sequence:   s.request.Sequence,
		EndOfBatch: s.request.EndOfBatch,
		Node: event.Node{
			GraphName: handler.graphName,
			NodeName:  name,
			NodeLabel: planNode.Label,
		},
		Runtime: s.runtime,
	}

	var handlerErr error
	started := time.Now()
	handler.emitMetric(RuntimeGraphMetric{
		Kind:      "node_scheduled",
		GraphName: handler.graphName,
		Node:      request.Node,
		Sequence:  request.Sequence,
	})
	for {
		handlerErr = s.invokeHandler(planNode.Handler, request)
		if handlerErr == nil {
			break
		}
		action := handler.handleRuntimeException(RuntimeGraphExceptionRequest[T]{
			Context:   request.Context,
			Event:     request.Event,
			Sequence:  request.Sequence,
			GraphName: handler.graphName,
			NodeName:  name,
			Kind:      RuntimeGraphExceptionKindHandler,
			Cause:     handlerErr,
			Runtime:   s.runtime,
		})
		switch action {
		case event.ExceptionActionContinue:
		case event.ExceptionActionRetry:
			continue
		default:
			return handlerErr
		}
		break
	}

	nodeState.done = true
	handler.emitMetric(RuntimeGraphMetric{
		Kind:      "node_completed",
		GraphName: handler.graphName,
		Node:      request.Node,
		Sequence:  request.Sequence,
		Duration:  time.Since(started),
	})
	for _, edge := range planNode.Outgoing {
		selected, err := edge.Evaluate(topology.EdgeConditionRequest[T]{
			Context:   request.Context,
			Event:     request.Event,
			Sequence:  request.Sequence,
			GraphName: handler.graphName,
			From:      edge.From,
			To:        edge.To,
			Runtime:   s.runtime,
		})
		if err != nil {
			action := handler.handleRuntimeException(RuntimeGraphExceptionRequest[T]{
				Context:   request.Context,
				Event:     request.Event,
				Sequence:  request.Sequence,
				GraphName: handler.graphName,
				NodeName:  name,
				EdgeFrom:  edge.From,
				EdgeTo:    edge.To,
				Kind:      RuntimeGraphExceptionKindCondition,
				Cause:     err,
				Runtime:   s.runtime,
			})
			if action != event.ExceptionActionContinue {
				return err
			}
			selected = false
		}
		handler.emitMetric(RuntimeGraphMetric{
			Kind:      runtimeGraphEdgeMetricKind(selected),
			GraphName: handler.graphName,
			Node:      request.Node,
			EdgeFrom:  edge.From,
			EdgeTo:    edge.To,
			Sequence:  request.Sequence,
			Selected:  selected,
		})
		if edge.To == graph.EndNode {
			if selected {
				s.endReached = true
			}
			continue
		}
		if err := s.resolveInbound(handler, edge.To, selected); err != nil {
			return err
		}
	}

	return nil
}

func (s *runtimeGraphRunState[T]) invokeHandler(
	handler event.Handler[T],
	request event.Request[T],
) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("disruptor: runtime graph handler panic: %v", recovered)
		}
	}()

	return handler.OnEvent(request)
}

// NewFatalRuntimeGraphExceptionHandler returns a handler that halts on every failure.
func NewFatalRuntimeGraphExceptionHandler[T any]() RuntimeGraphExceptionHandler[T] {
	return runtimeGraphExceptionHandlerFunc[T](func(RuntimeGraphExceptionRequest[T]) event.ExceptionAction {
		return event.ExceptionActionHalt
	})
}

type runtimeGraphExceptionHandlerFunc[T any] func(RuntimeGraphExceptionRequest[T]) event.ExceptionAction

func (fn runtimeGraphExceptionHandlerFunc[T]) HandleRuntimeGraphException(
	request RuntimeGraphExceptionRequest[T],
) event.ExceptionAction {
	if fn == nil {
		return event.ExceptionActionHalt
	}

	return fn(request)
}

func runtimeGraphEdgeMetricKind(selected bool) string {
	if selected {
		return "edge_selected"
	}

	return "edge_skipped"
}
