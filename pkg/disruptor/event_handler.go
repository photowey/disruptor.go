package disruptor

import (
	"context"
	"fmt"
)

type EventHandler[T any] interface {
	OnEvent(request EventRequest[T]) error
}

type EventHandlerFunc[T any] func(request EventRequest[T]) error

func (fn EventHandlerFunc[T]) OnEvent(request EventRequest[T]) error {
	return fn(request)
}

type EventRequest[T any] struct {
	Context    context.Context
	Event      *T
	Sequence   int64
	EndOfBatch bool
}

type BatchStartHandler interface {
	OnBatchStart(request BatchStartRequest) error
}

type BatchStartRequest struct {
	Context    context.Context
	BatchSize  int64
	QueueDepth int64
}

type LifecycleHandler interface {
	OnStart(ctx context.Context) error
	OnShutdown(ctx context.Context) error
}

type ExceptionAction uint8

const (
	ExceptionActionUnknown ExceptionAction = iota
	ExceptionActionHalt
	ExceptionActionContinue
	ExceptionActionRetry
)

type ExceptionHandler[T any] interface {
	HandleEventException(request EventException[T]) ExceptionAction
	HandleStartException(request LifecycleException) ExceptionAction
	HandleShutdownException(request LifecycleException) ExceptionAction
}

type EventException[T any] struct {
	Context  context.Context
	Event    *T
	Sequence int64
	Err      error
}

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

type ProcessorOption[T any] interface {
	applyProcessor(config *processorConfig[T]) error
}

type processorConfig[T any] struct {
	exceptionHandler ExceptionHandler[T]
}

type processorOptionFunc[T any] struct {
	applyFunc func(*processorConfig[T]) error
}

func (fn processorOptionFunc[T]) applyProcessor(config *processorConfig[T]) error {
	return fn.applyFunc(config)
}

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
