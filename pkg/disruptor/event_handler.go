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
	"sync"
)

// EventHandler consumes events produced by the ring buffer.
type EventHandler[T any] interface {
	OnEvent(request EventRequest[T]) error
}

// EventHandlerFunc adapts a function to the EventHandler interface.
type EventHandlerFunc[T any] func(request EventRequest[T]) error

// OnEvent calls the wrapped handler function.
func (fn EventHandlerFunc[T]) OnEvent(request EventRequest[T]) error {
	return fn(request)
}

// EventRequest provides the current event, sequence, and batch context.
type EventRequest[T any] struct {
	Context    context.Context
	Event      *T
	Sequence   int64
	EndOfBatch bool
}

// BatchStartHandler is notified before a batch of events is processed.
type BatchStartHandler interface {
	OnBatchStart(request BatchStartRequest) error
}

// BatchStartRequest describes the batch that is about to be processed.
type BatchStartRequest struct {
	Context    context.Context
	BatchSize  int64
	QueueDepth int64
}

// LifecycleHandler observes processor start and shutdown transitions.
type LifecycleHandler interface {
	OnStart(ctx context.Context) error
	OnShutdown(ctx context.Context) error
}

// ExceptionAction defines how a processor should react to a failure.
type ExceptionAction uint8

const (
	// ExceptionActionUnknown is the zero value and should not be returned.
	ExceptionActionUnknown ExceptionAction = iota
	// ExceptionActionHalt stops the processor and reports the error.
	ExceptionActionHalt
	// ExceptionActionContinue advances past the failed sequence.
	ExceptionActionContinue
	// ExceptionActionRetry retries the same sequence without advancing.
	ExceptionActionRetry
)

// ExceptionHandler decides how event and lifecycle failures are handled.
type ExceptionHandler[T any] interface {
	HandleEventException(request EventException[T]) ExceptionAction
	HandleStartException(request LifecycleException) ExceptionAction
	HandleShutdownException(request LifecycleException) ExceptionAction
}

// EventException reports an event handling failure.
type EventException[T any] struct {
	Context  context.Context
	Event    *T
	Sequence int64
	Err      error
}

// LifecycleException reports a start or shutdown failure.
type LifecycleException struct {
	Context context.Context
	Err     error
}

type exceptionHandlerFunc[T any] struct {
	handleEvent    func(EventException[T]) ExceptionAction
	handleStart    func(LifecycleException) ExceptionAction
	handleShutdown func(LifecycleException) ExceptionAction
}

func (f exceptionHandlerFunc[T]) HandleEventException(request EventException[T]) ExceptionAction {
	if f.handleEvent == nil {
		return ExceptionActionHalt
	}

	return f.handleEvent(request)
}

func (f exceptionHandlerFunc[T]) HandleStartException(request LifecycleException) ExceptionAction {
	if f.handleStart == nil {
		return ExceptionActionHalt
	}

	return f.handleStart(request)
}

func (f exceptionHandlerFunc[T]) HandleShutdownException(request LifecycleException) ExceptionAction {
	if f.handleShutdown == nil {
		return ExceptionActionHalt
	}

	return f.handleShutdown(request)
}

// ProcessorOption configures a batch event processor.
type ProcessorOption[T any] interface {
	applyProcessor(config *processorConfig[T]) error
}

type processorConfig[T any] struct {
	exceptionHandler ExceptionHandler[T]
}

type processorOptionFunc[T any] struct {
	applyFunc func(*processorConfig[T]) error
}

//nolint:unused // The method satisfies ProcessorOption[T] and is called through the interface.
func (fn processorOptionFunc[T]) applyProcessor(config *processorConfig[T]) error {
	return fn.applyFunc(config)
}

// WithExceptionHandler sets the processor exception handler.
func WithExceptionHandler[T any](handler ExceptionHandler[T]) ProcessorOption[T] {
	return processorOptionFunc[T]{
		applyFunc: func(config *processorConfig[T]) error {
			if handler == nil {
				return fmt.Errorf("disruptor: exception handler is nil")
			}

			config.exceptionHandler = handler
			return nil
		},
	}
}

func defaultProcessorConfig[T any]() processorConfig[T] {
	return processorConfig[T]{
		exceptionHandler: NewFatalExceptionHandler[T](),
	}
}

// NewFatalExceptionHandler returns a handler that halts on every failure.
func NewFatalExceptionHandler[T any]() ExceptionHandler[T] {
	return exceptionHandlerFunc[T]{
		handleEvent: func(EventException[T]) ExceptionAction {
			return ExceptionActionHalt
		},
		handleStart: func(LifecycleException) ExceptionAction {
			return ExceptionActionHalt
		},
		handleShutdown: func(LifecycleException) ExceptionAction {
			return ExceptionActionHalt
		},
	}
}

// NewIgnoreExceptionHandler returns a handler that continues after failures.
func NewIgnoreExceptionHandler[T any]() ExceptionHandler[T] {
	return exceptionHandlerFunc[T]{
		handleEvent: func(EventException[T]) ExceptionAction {
			return ExceptionActionContinue
		},
		handleStart: func(LifecycleException) ExceptionAction {
			return ExceptionActionContinue
		},
		handleShutdown: func(LifecycleException) ExceptionAction {
			return ExceptionActionContinue
		},
	}
}

// RetryExceptionHandler retries failed events before using an exhausted action.
type RetryExceptionHandler[T any] struct {
	mu              sync.Mutex
	maxRetries      int
	exhaustedAction ExceptionAction
	attempts        map[int64]int
}

// NewRetryExceptionHandler creates a bounded retry exception handler.
func NewRetryExceptionHandler[T any](
	maxRetries int,
	exhaustedAction ExceptionAction,
) (*RetryExceptionHandler[T], error) {
	if maxRetries < 0 {
		return nil, fmt.Errorf("disruptor: max retries must be non-negative")
	}
	if exhaustedAction == ExceptionActionUnknown || exhaustedAction == ExceptionActionRetry {
		return nil, fmt.Errorf("disruptor: invalid exhausted retry action")
	}

	return &RetryExceptionHandler[T]{
		maxRetries:      maxRetries,
		exhaustedAction: exhaustedAction,
		attempts:        make(map[int64]int),
	}, nil
}

// HandleEventException returns retry until the configured retry budget is exhausted.
func (h *RetryExceptionHandler[T]) HandleEventException(
	request EventException[T],
) ExceptionAction {
	h.mu.Lock()
	defer h.mu.Unlock()

	attempts := h.attempts[request.Sequence]
	if attempts < h.maxRetries {
		h.attempts[request.Sequence] = attempts + 1
		return ExceptionActionRetry
	}

	delete(h.attempts, request.Sequence)
	return h.exhaustedAction
}

// HandleStartException halts processors when lifecycle start fails.
func (h *RetryExceptionHandler[T]) HandleStartException(
	request LifecycleException,
) ExceptionAction {
	return ExceptionActionHalt
}

// HandleShutdownException halts processors when lifecycle shutdown fails.
func (h *RetryExceptionHandler[T]) HandleShutdownException(
	request LifecycleException,
) ExceptionAction {
	return ExceptionActionHalt
}

func (h *RetryExceptionHandler[T]) resetRetry(sequence int64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	delete(h.attempts, sequence)
}
