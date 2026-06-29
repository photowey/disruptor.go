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

package processor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/photowey/disruptor.go/pkg/event"
	"github.com/photowey/disruptor.go/pkg/metrics"
	"github.com/photowey/disruptor.go/pkg/ringbuffer"
	"github.com/photowey/disruptor.go/pkg/runtimevars"
	"github.com/photowey/disruptor.go/pkg/sequence"
)

// EventProcessor controls a processor lifecycle and exposes its sequence.
type EventProcessor interface {
	Start(ctx context.Context) error
	Stop()
	Wait() error
	Sequence() *sequence.Sequence
}

// HaltNotifier receives graph-wide halt signals from a processor.
type HaltNotifier interface {
	NotifyHalt()
}

// BatchConfig configures internal batch processor behavior.
type BatchConfig[T any] struct {
	ExceptionHandler event.ExceptionHandler[T]
	ProducerGating   bool
	HaltAdvances     bool
	Node             event.Node
	HaltNotifier     HaltNotifier
}

// BatchEventProcessor waits for published events and dispatches batches.
type BatchEventProcessor[T any] struct {
	ringBuffer       *ringbuffer.RingBuffer[T]
	barrier          ringbuffer.Barrier
	handler          event.Handler[T]
	exceptionHandler event.ExceptionHandler[T]
	node             event.Node
	producerGating   bool
	haltAdvances     bool
	haltNotifier     HaltNotifier

	sequence *sequence.Sequence

	cancel  context.CancelFunc
	wg      sync.WaitGroup
	started atomic.Bool
	stopped atomic.Bool

	terminalErr atomic.Value
}

// NewBatchEventProcessor creates a processor for one event handler.
func NewBatchEventProcessor[T any](
	ringBuffer *ringbuffer.RingBuffer[T],
	barrier ringbuffer.Barrier,
	handler event.Handler[T],
	opts ...Option[T],
) (*BatchEventProcessor[T], error) {
	return NewBatchEventProcessorWithConfig(
		ringBuffer,
		barrier,
		handler,
		BatchConfig[T]{
			ExceptionHandler: defaultOptions[T]().exceptionHandler,
			ProducerGating:   true,
			HaltAdvances:     true,
		},
		opts...,
	)
}

// NewBatchEventProcessorWithConfig creates a processor with explicit internal behavior.
func NewBatchEventProcessorWithConfig[T any](
	ringBuffer *ringbuffer.RingBuffer[T],
	barrier ringbuffer.Barrier,
	handler event.Handler[T],
	config BatchConfig[T],
	opts ...Option[T],
) (*BatchEventProcessor[T], error) {
	if ringBuffer == nil {
		return nil, fmt.Errorf("processor: ring buffer is nil")
	}
	if barrier == nil {
		return nil, fmt.Errorf("processor: barrier is nil")
	}
	if handler == nil {
		return nil, fmt.Errorf("processor: event handler is nil")
	}

	if config.ExceptionHandler == nil {
		config.ExceptionHandler = defaultOptions[T]().exceptionHandler
	}

	processorOptions := options[T]{
		exceptionHandler: config.ExceptionHandler,
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(&processorOptions); err != nil {
			return nil, fmt.Errorf("applying processor option: %w", err)
		}
	}
	config.ExceptionHandler = processorOptions.exceptionHandler

	processor := &BatchEventProcessor[T]{
		ringBuffer:       ringBuffer,
		barrier:          barrier,
		handler:          handler,
		exceptionHandler: config.ExceptionHandler,
		node:             config.Node,
		producerGating:   config.ProducerGating,
		haltAdvances:     config.HaltAdvances,
		haltNotifier:     config.HaltNotifier,
		sequence:         sequence.New(sequence.InitialValue),
	}
	if processor.producerGating {
		ringBuffer.AddGatingSequences(processor.sequence)
	}

	return processor, nil
}

// Start launches the processor goroutine.
func (p *BatchEventProcessor[T]) Start(ctx context.Context) error {
	if !p.started.CompareAndSwap(false, true) {
		return ErrClosed
	}

	processorCtx, cancel := context.WithCancel(ctx)
	p.cancel = cancel
	p.sequence.Store(sequence.InitialValue)
	p.wg.Add(1)
	p.processorStateMetric("running", nil)
	go p.run(processorCtx)

	return nil
}

// Stop requests the processor to halt and wakes any blocked waits.
func (p *BatchEventProcessor[T]) Stop() {
	if p.stopped.Swap(true) {
		return
	}

	if p.cancel != nil {
		p.cancel()
	}
	if p.barrier != nil {
		p.barrier.Alert()
	}
	if p.ringBuffer != nil {
		p.ringBuffer.SignalAll()
	}
}

// Wait waits for the processor goroutine and returns its terminal error.
func (p *BatchEventProcessor[T]) Wait() error {
	p.wg.Wait()

	if value := p.terminalErr.Load(); value != nil {
		if err, ok := value.(error); ok {
			return err
		}
	}

	return nil
}

// Sequence returns the processor's current consumer sequence.
func (p *BatchEventProcessor[T]) Sequence() *sequence.Sequence {
	return p.sequence
}

// RemoveGatingSequence unregisters the processor sequence from producer backpressure.
func (p *BatchEventProcessor[T]) RemoveGatingSequence() {
	p.removeGatingSequence()
}

func (p *BatchEventProcessor[T]) run(ctx context.Context) {
	defer p.wg.Done()
	defer p.removeGatingSequence()
	defer p.processorStateMetric("stopped", nil)

	if !p.notifyLifecycleStart(ctx) {
		return
	}
	defer p.notifyLifecycleShutdown(ctx)

	nextSequence := p.sequence.Value() + 1
	for {
		availableSequence, err := p.barrier.WaitFor(ctx, nextSequence)
		if err != nil {
			if p.stopped.Load() || errors.Is(err, context.Canceled) || errors.Is(err, ringbuffer.ErrAlerted) {
				p.storeTerminalErr(nil)
				return
			}

			p.storeTerminalErr(err)
			return
		}

		batchRequest := event.BatchStartRequest{
			Context:    ctx,
			BatchSize:  availableSequence - nextSequence + 1,
			QueueDepth: availableSequence - nextSequence + 1,
			Node:       p.node,
		}
		if !p.notifyBatchStart(batchRequest) {
			return
		}
		p.batchStartMetric(batchRequest)
		for nextSequence <= availableSequence {
			request := event.Request[T]{
				Context:    ctx,
				Event:      p.ringBuffer.Get(nextSequence),
				Sequence:   nextSequence,
				EndOfBatch: nextSequence == availableSequence,
				Node:       p.node,
				Runtime:    runtimevars.NoopContext{},
			}

			var started time.Time
			if p.ringBuffer.Metrics() != nil {
				started = time.Now()
			}
			err := p.invokeHandler(request)
			p.eventHandledMetric(nextSequence, started, err)
			if err != nil {
				action := p.exceptionHandler.HandleEventException(event.Exception[T]{
					Context:  ctx,
					Event:    request.Event,
					Sequence: nextSequence,
					Err:      err,
					Node:     p.node,
				})

				switch action {
				case event.ExceptionActionContinue:
					p.storeSequence(nextSequence)
					nextSequence++
					continue
				case event.ExceptionActionRetry:
					continue
				default:
					if p.haltAdvances {
						p.storeSequence(nextSequence)
					}
					p.storeTerminalErr(err)
					p.notifyHalt()
					return
				}
			}

			p.resetRetryState(nextSequence)
			p.storeSequence(nextSequence)
			nextSequence++
		}
	}
}

func (p *BatchEventProcessor[T]) invokeHandler(request event.Request[T]) (err error) {
	defer recoverHandlerPanic(&err)

	return p.handler.OnEvent(request)
}

func (p *BatchEventProcessor[T]) invokeBatchStart(
	handler event.BatchStartHandler,
	request event.BatchStartRequest,
) (err error) {
	defer recoverBatchStartPanic(&err)

	return handler.OnBatchStart(request)
}

func (p *BatchEventProcessor[T]) invokeLifecycleStart(
	ctx context.Context,
	handler event.LifecycleHandler,
) (err error) {
	defer recoverLifecycleStartPanic(&err)

	return handler.OnStart(ctx)
}

func (p *BatchEventProcessor[T]) invokeLifecycleShutdown(
	ctx context.Context,
	handler event.LifecycleHandler,
) (err error) {
	defer recoverLifecycleShutdownPanic(&err)

	return handler.OnShutdown(ctx)
}

func recoverHandlerPanic(err *error) {
	if recovered := recover(); recovered != nil {
		*err = fmt.Errorf("processor: handler panic: %v", recovered)
	}
}

func recoverBatchStartPanic(err *error) {
	if recovered := recover(); recovered != nil {
		*err = fmt.Errorf("processor: batch start panic: %v", recovered)
	}
}

func recoverLifecycleStartPanic(err *error) {
	if recovered := recover(); recovered != nil {
		*err = fmt.Errorf("processor: lifecycle start panic: %v", recovered)
	}
}

func recoverLifecycleShutdownPanic(err *error) {
	if recovered := recover(); recovered != nil {
		*err = fmt.Errorf("processor: lifecycle shutdown panic: %v", recovered)
	}
}

func (p *BatchEventProcessor[T]) storeTerminalErr(err error) {
	if err == nil {
		return
	}

	if value := p.terminalErr.Load(); value != nil {
		if existing, ok := value.(error); ok && existing != nil {
			p.terminalErr.Store(errors.Join(existing, err))
			return
		}
	}

	p.terminalErr.Store(err)
}

func (p *BatchEventProcessor[T]) notifyLifecycleStart(ctx context.Context) bool {
	handler, ok := p.handler.(event.LifecycleHandler)
	if !ok {
		return true
	}

	for {
		err := p.invokeLifecycleStart(ctx, handler)
		if err == nil {
			return true
		}

		action := p.exceptionHandler.HandleStartException(event.LifecycleException{
			Context: ctx,
			Err:     err,
			Node:    p.node,
		})
		switch action {
		case event.ExceptionActionContinue:
			return true
		case event.ExceptionActionRetry:
			continue
		default:
			p.storeTerminalErr(err)
			return false
		}
	}
}

func (p *BatchEventProcessor[T]) notifyLifecycleShutdown(ctx context.Context) {
	handler, ok := p.handler.(event.LifecycleHandler)
	if !ok {
		return
	}

	for {
		err := p.invokeLifecycleShutdown(ctx, handler)
		if err == nil {
			return
		}

		action := p.exceptionHandler.HandleShutdownException(event.LifecycleException{
			Context: ctx,
			Err:     err,
			Node:    p.node,
		})
		switch action {
		case event.ExceptionActionContinue:
			return
		case event.ExceptionActionRetry:
			continue
		default:
			p.storeTerminalErr(err)
			return
		}
	}
}

func (p *BatchEventProcessor[T]) notifyBatchStart(request event.BatchStartRequest) bool {
	handler, ok := p.handler.(event.BatchStartHandler)
	if !ok {
		return true
	}

	for {
		err := p.invokeBatchStart(handler, request)
		if err == nil {
			return true
		}

		action := p.exceptionHandler.HandleStartException(event.LifecycleException{
			Context: request.Context,
			Err:     err,
			Node:    p.node,
		})
		switch action {
		case event.ExceptionActionContinue:
			return true
		case event.ExceptionActionRetry:
			continue
		default:
			p.storeTerminalErr(err)
			return false
		}
	}
}

func (p *BatchEventProcessor[T]) batchStartMetric(request event.BatchStartRequest) {
	sink := p.ringBuffer.Metrics()
	if sink == nil {
		return
	}

	sink.OnBatchStart(metrics.BatchMetric{
		BatchSize:  request.BatchSize,
		QueueDepth: request.QueueDepth,
		Node:       p.node,
	})
}

func (p *BatchEventProcessor[T]) eventHandledMetric(sequenceValue int64, started time.Time, err error) {
	sink := p.ringBuffer.Metrics()
	if sink == nil {
		return
	}

	sink.OnEventHandled(metrics.EventMetric{
		Sequence: sequenceValue,
		Duration: time.Since(started),
		Err:      err,
		Node:     p.node,
	})
}

func (p *BatchEventProcessor[T]) processorStateMetric(state string, err error) {
	sink := p.ringBuffer.Metrics()
	if sink == nil {
		return
	}

	sink.OnProcessorState(metrics.ProcessorMetric{
		State: state,
		Err:   err,
		Node:  p.node,
	})
}

func (p *BatchEventProcessor[T]) notifyHalt() {
	if p.haltNotifier == nil {
		return
	}

	p.haltNotifier.NotifyHalt()
}

func (p *BatchEventProcessor[T]) resetRetryState(sequenceValue int64) {
	resetter, ok := p.exceptionHandler.(interface{ ResetRetry(sequence int64) })
	if !ok {
		return
	}

	resetter.ResetRetry(sequenceValue)
}

func (p *BatchEventProcessor[T]) storeSequence(sequenceValue int64) {
	p.sequence.Store(sequenceValue)
	p.signalWaiters()
}

func (p *BatchEventProcessor[T]) removeGatingSequence() {
	if p.ringBuffer == nil || !p.producerGating {
		return
	}
	if p.ringBuffer.RemoveGatingSequence(p.sequence) {
		p.signalWaiters()
	}
}

func (p *BatchEventProcessor[T]) signalWaiters() {
	if p.ringBuffer == nil {
		return
	}

	p.ringBuffer.SignalAll()
}
