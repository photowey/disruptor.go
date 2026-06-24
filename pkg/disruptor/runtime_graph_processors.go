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
	Runtime   RuntimeContext
}

// RuntimeGraphExceptionHandler decides how runtime graph failures are handled.
type RuntimeGraphExceptionHandler[T any] interface {
	HandleRuntimeGraphException(request RuntimeGraphExceptionRequest[T]) ExceptionAction
}

// RuntimeGraphExceptionHandlerFunc adapts a function to RuntimeGraphExceptionHandler.
type RuntimeGraphExceptionHandlerFunc[T any] func(
	request RuntimeGraphExceptionRequest[T],
) ExceptionAction

// HandleRuntimeGraphException calls the wrapped function.
func (fn RuntimeGraphExceptionHandlerFunc[T]) HandleRuntimeGraphException(
	request RuntimeGraphExceptionRequest[T],
) ExceptionAction {
	if fn == nil {
		return ExceptionActionHalt
	}

	return fn(request)
}

// RuntimeNoRouteAction determines how a no-route runtime graph outcome is handled.
type RuntimeNoRouteAction uint8

const (
	// RuntimeNoRouteActionHalt stops the processor and reports ErrRuntimeNoRoute.
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
	provider         RuntimeVariablesProvider[T]
	resolver         EventValueResolver[T]
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
				return fmt.Errorf("%w: runtime graph exception handler is nil", ErrInvalidGraph)
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
				return fmt.Errorf("%w: runtime graph workers must be positive", ErrInvalidGraph)
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
				return fmt.Errorf("%w: invalid runtime graph no-route action", ErrInvalidGraph)
			}
		},
	}
}

// WithRuntimeGraphVariablesProvider sets a runtime variables provider.
func WithRuntimeGraphVariablesProvider[T any](
	provider RuntimeVariablesProvider[T],
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
	resolver EventValueResolver[T],
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
	Node      NodeContext
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
	Snapshot() RuntimeGraphSnapshot
}

type handledRuntimeGraphProcessors struct {
	processor EventProcessor
	snapshot  RuntimeGraphSnapshot
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

func (p *handledRuntimeGraphProcessors) Snapshot() RuntimeGraphSnapshot {
	return runtimeGraphSnapshotCopy(p.snapshot)
}

// HandleRuntimeGraph registers a runtime graph scheduler.
func (d *Disruptor[T]) HandleRuntimeGraph(
	graph *RuntimeGraph[T],
	opts ...RuntimeGraphHandleOption[T],
) (RuntimeGraphProcessors, error) {
	if graph == nil {
		return nil, fmt.Errorf("%w: runtime graph is nil", ErrInvalidGraph)
	}

	handleConfig := runtimeGraphHandleConfig[T]{
		exceptionHandler: NewFatalRuntimeGraphExceptionHandler[T](),
		noRouteAction:    RuntimeNoRouteActionHalt,
		workers:          1,
		resolver:         newReflectionEventValueResolver[T](),
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

	graph.mu.Lock()
	defer graph.mu.Unlock()
	if graph.handled {
		return nil, ErrGraphHandled
	}
	if err := graph.validateLocked(); err != nil {
		return nil, err
	}

	if handleConfig.metricsSink == nil {
		if metricsSink, ok := d.ringBuffer.metrics.(RuntimeGraphMetricsSink); ok {
			handleConfig.metricsSink = metricsSink
		}
	}

	plan := newRuntimeGraphPlan(graph)
	handler := &runtimeGraphEventHandler[T]{
		graphName:        graph.name,
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

	processor, err := newBatchEventProcessor(
		d.ringBuffer,
		d.ringBuffer.NewBarrier(),
		handler,
		batchEventProcessorConfig[T]{
			exceptionHandler: defaultProcessorConfig[T]().exceptionHandler,
			producerGating:   true,
			haltAdvances:     false,
			node: NodeContext{
				GraphName: graph.name,
				NodeName:  "scheduler",
				NodeLabel: graph.name,
			},
			onHalt: stopGraph,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("creating runtime graph processor: %w", err)
	}

	graph.freezeHandledLocked()
	d.mode = consumerModeGraph
	d.processors = append(d.processors, processor)

	return &handledRuntimeGraphProcessors{
		processor: processor,
		snapshot:  graph.snapshotLocked(),
	}, nil
}

type runtimeGraphPlan[T any] struct {
	snapshot RuntimeGraphSnapshot
	nodes    map[string]*runtimeGraphPlanNode[T]
	start    []runtimeGraphPlanEdge[T]
}

type runtimeGraphPlanNode[T any] struct {
	node     *graphNode[T]
	incoming int
	outgoing []runtimeGraphPlanEdge[T]
}

type runtimeGraphPlanEdge[T any] struct {
	from              string
	to                string
	condition         EdgeCondition[T]
	compiledCondition BoolExpression
}

func (e runtimeGraphPlanEdge[T]) evaluate(request EdgeConditionRequest[T]) (bool, error) {
	if e.compiledCondition != nil {
		return e.compiledCondition.EvaluateBool(ExpressionRequest{
			Context:   request.Context,
			Variables: request.Runtime.Variables(),
		})
	}
	if e.condition == nil {
		return true, nil
	}

	return e.condition.Evaluate(request)
}

func newRuntimeGraphPlan[T any](graph *RuntimeGraph[T]) *runtimeGraphPlan[T] {
	nodes := make(map[string]*runtimeGraphPlanNode[T], len(graph.nodes))
	for name, node := range graph.nodes {
		nodes[name] = &runtimeGraphPlanNode[T]{
			node: node,
		}
	}

	edgesByFrom := make(map[string][]runtimeGraphPlanEdge[T])
	startEdges := make([]runtimeGraphPlanEdge[T], 0)
	for key, edge := range graph.edges {
		planEdge := runtimeGraphPlanEdge[T]{
			from:              key.From,
			to:                key.To,
			condition:         edge.condition,
			compiledCondition: edge.compiledCondition,
		}
		if key.From == GraphStartNode {
			startEdges = append(startEdges, planEdge)
		} else {
			edgesByFrom[key.From] = append(edgesByFrom[key.From], planEdge)
		}
		if key.To != GraphEndNode {
			nodes[key.To].incoming++
		}
	}
	sort.Slice(startEdges, func(i, j int) bool {
		return startEdges[i].to < startEdges[j].to
	})
	for from := range edgesByFrom {
		sort.Slice(edgesByFrom[from], func(i, j int) bool {
			return edgesByFrom[from][i].to < edgesByFrom[from][j].to
		})
		nodes[from].outgoing = append(nodes[from].outgoing, edgesByFrom[from]...)
	}
	for name, node := range nodes {
		if node.node == nil {
			continue
		}
		if node.incoming == 0 {
			// The validation path guarantees at least one incoming edge, but the
			// scheduler keeps the zero value safe.
			node.incoming = 1
		}
		_ = name
	}

	snapshot := graph.snapshotLocked()
	return &runtimeGraphPlan[T]{
		snapshot: snapshot,
		nodes:    nodes,
		start:    startEdges,
	}
}

type runtimeGraphEventHandler[T any] struct {
	graphName        string
	plan             *runtimeGraphPlan[T]
	exceptionHandler RuntimeGraphExceptionHandler[T]
	noRouteAction    RuntimeNoRouteAction
	provider         RuntimeVariablesProvider[T]
	resolver         EventValueResolver[T]
	metricsSink      RuntimeGraphMetricsSink
	workers          int
}

func (h *runtimeGraphEventHandler[T]) OnStart(ctx context.Context) error {
	if h.workers < 1 {
		return fmt.Errorf("%w: runtime graph workers must be positive", ErrInvalidGraph)
	}

	return nil
}

func (h *runtimeGraphEventHandler[T]) OnShutdown(ctx context.Context) error {
	return nil
}

func (h *runtimeGraphEventHandler[T]) OnEvent(request EventRequest[T]) error {
	var providerVariables RuntimeVariables
	if h.provider != nil {
		var err error
		providerVariables, err = h.provider.Variables(RuntimeVariablesRequest[T]{
			Context:   request.Context,
			Event:     request.Event,
			Sequence:  request.Sequence,
			GraphName: h.graphName,
		})
		if err != nil {
			return err
		}
	}

	runtimeCtx := newRuntimeContextWithResolver(request, h.graphName, providerVariables, h.resolver)
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
			Cause:     ErrRuntimeNoRoute,
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
) ExceptionAction {
	h.emitMetric(RuntimeGraphMetric{
		Kind:      "exception",
		GraphName: request.GraphName,
		Node: NodeContext{
			GraphName: request.GraphName,
			NodeName:  request.NodeName,
		},
		EdgeFrom: request.EdgeFrom,
		EdgeTo:   request.EdgeTo,
		Sequence: request.Sequence,
		Err:      request.Cause,
	})
	if h.exceptionHandler == nil {
		return ExceptionActionHalt
	}

	action := h.exceptionHandler.HandleRuntimeGraphException(request)
	if action == ExceptionActionUnknown {
		return ExceptionActionHalt
	}

	return action
}

func (h *runtimeGraphEventHandler[T]) raiseRuntimeException(
	request RuntimeGraphExceptionRequest[T],
) error {
	if h.handleRuntimeException(request) == ExceptionActionContinue {
		return nil
	}

	return request.Cause
}

type runtimeGraphRunState[T any] struct {
	plan       *runtimeGraphPlan[T]
	runtime    RuntimeContext
	request    EventRequest[T]
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
	plan *runtimeGraphPlan[T],
	runtime RuntimeContext,
	request EventRequest[T],
) *runtimeGraphRunState[T] {
	nodes := make(map[string]*runtimeGraphNodeState, len(plan.nodes))
	for name, node := range plan.nodes {
		nodes[name] = &runtimeGraphNodeState{total: node.incoming}
	}

	return &runtimeGraphRunState[T]{
		plan:    plan,
		runtime: runtime,
		request: request,
		nodes:   nodes,
		ready:   make([]string, 0, len(plan.nodes)),
	}
}

func (s *runtimeGraphRunState[T]) processStart(handler *runtimeGraphEventHandler[T]) error {
	for _, edge := range s.plan.start {
		selected, err := edge.evaluate(EdgeConditionRequest[T]{
			Context:   s.request.Context,
			Event:     s.request.Event,
			Sequence:  s.request.Sequence,
			GraphName: handler.graphName,
			From:      edge.from,
			To:        edge.to,
			Runtime:   s.runtime,
		})
		if err != nil {
			action := handler.handleRuntimeException(RuntimeGraphExceptionRequest[T]{
				Context:   s.request.Context,
				Event:     s.request.Event,
				Sequence:  s.request.Sequence,
				GraphName: handler.graphName,
				EdgeFrom:  edge.from,
				EdgeTo:    edge.to,
				Kind:      RuntimeGraphExceptionKindCondition,
				Cause:     err,
				Runtime:   s.runtime,
			})
			if action != ExceptionActionContinue {
				return err
			}
			selected = false
		}
		handler.emitMetric(RuntimeGraphMetric{
			Kind:      runtimeGraphEdgeMetricKind(selected),
			GraphName: handler.graphName,
			EdgeFrom:  edge.from,
			EdgeTo:    edge.to,
			Sequence:  s.request.Sequence,
			Selected:  selected,
		})
		if edge.to == GraphEndNode {
			if selected {
				s.endReached = true
			}
			continue
		}
		if err := s.resolveInbound(handler, edge.to, selected); err != nil {
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
			Node: NodeContext{
				GraphName: handler.graphName,
				NodeName:  name,
			},
			Sequence: s.request.Sequence,
		})
		for _, edge := range s.plan.nodes[name].outgoing {
			if edge.to == GraphEndNode {
				continue
			}
			if err := s.resolveInbound(handler, edge.to, false); err != nil {
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

	planNode := s.plan.nodes[name]
	request := EventRequest[T]{
		Context:    s.request.Context,
		Event:      s.request.Event,
		Sequence:   s.request.Sequence,
		EndOfBatch: s.request.EndOfBatch,
		Node: NodeContext{
			GraphName: handler.graphName,
			NodeName:  name,
			NodeLabel: planNode.node.label,
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
		handlerErr = s.invokeHandler(planNode.node.handler, request)
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
		case ExceptionActionContinue:
		case ExceptionActionRetry:
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
	for _, edge := range planNode.outgoing {
		selected, err := edge.evaluate(EdgeConditionRequest[T]{
			Context:   request.Context,
			Event:     request.Event,
			Sequence:  request.Sequence,
			GraphName: handler.graphName,
			From:      edge.from,
			To:        edge.to,
			Runtime:   s.runtime,
		})
		if err != nil {
			action := handler.handleRuntimeException(RuntimeGraphExceptionRequest[T]{
				Context:   request.Context,
				Event:     request.Event,
				Sequence:  request.Sequence,
				GraphName: handler.graphName,
				NodeName:  name,
				EdgeFrom:  edge.from,
				EdgeTo:    edge.to,
				Kind:      RuntimeGraphExceptionKindCondition,
				Cause:     err,
				Runtime:   s.runtime,
			})
			if action != ExceptionActionContinue {
				return err
			}
			selected = false
		}
		handler.emitMetric(RuntimeGraphMetric{
			Kind:      runtimeGraphEdgeMetricKind(selected),
			GraphName: handler.graphName,
			Node:      request.Node,
			EdgeFrom:  edge.from,
			EdgeTo:    edge.to,
			Sequence:  request.Sequence,
			Selected:  selected,
		})
		if edge.to == GraphEndNode {
			if selected {
				s.endReached = true
			}
			continue
		}
		if err := s.resolveInbound(handler, edge.to, selected); err != nil {
			return err
		}
	}

	return nil
}

func (s *runtimeGraphRunState[T]) invokeHandler(
	handler EventHandler[T],
	request EventRequest[T],
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
	return runtimeGraphExceptionHandlerFunc[T](func(RuntimeGraphExceptionRequest[T]) ExceptionAction {
		return ExceptionActionHalt
	})
}

type runtimeGraphExceptionHandlerFunc[T any] func(RuntimeGraphExceptionRequest[T]) ExceptionAction

func (fn runtimeGraphExceptionHandlerFunc[T]) HandleRuntimeGraphException(
	request RuntimeGraphExceptionRequest[T],
) ExceptionAction {
	if fn == nil {
		return ExceptionActionHalt
	}

	return fn(request)
}

func runtimeGraphSnapshotCopy(snapshot RuntimeGraphSnapshot) RuntimeGraphSnapshot {
	nodes := make([]GraphNodeSnapshot, len(snapshot.Nodes))
	for i, node := range snapshot.Nodes {
		nodes[i] = GraphNodeSnapshot{
			Name:     node.Name,
			Label:    node.Label,
			Metadata: copyStringMap(node.Metadata),
		}
	}
	edges := append([]RuntimeGraphEdgeSnapshot(nil), snapshot.Edges...)

	return RuntimeGraphSnapshot{
		Name:    snapshot.Name,
		Frozen:  snapshot.Frozen,
		Nodes:   nodes,
		Edges:   edges,
		Sources: append([]string(nil), snapshot.Sources...),
		Leaves:  append([]string(nil), snapshot.Leaves...),
		Entries: append([]string(nil), snapshot.Entries...),
		Exits:   append([]string(nil), snapshot.Exits...),
	}
}

func runtimeGraphEdgeMetricKind(selected bool) string {
	if selected {
		return "edge_selected"
	}

	return "edge_skipped"
}
