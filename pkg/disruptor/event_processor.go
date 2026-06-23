package disruptor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type EventProcessor interface {
	Start(ctx context.Context) error
	Stop()
	Wait() error
	Sequence() *Sequence
}

type BatchEventProcessor[T any] struct {
	ringBuffer       *RingBuffer[T]
	barrier          Barrier
	handler          EventHandler[T]
	exceptionHandler ExceptionHandler[T]

	sequence *Sequence

	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	started atomic.Bool
	stopped atomic.Bool

	terminalErr atomic.Value
}

func NewBatchEventProcessor[T any](
	ringBuffer *RingBuffer[T],
	barrier Barrier,
	handler EventHandler[T],
	opts ...ProcessorOption[T],
) (*BatchEventProcessor[T], error) {
	if ringBuffer == nil {
		return nil, fmt.Errorf("disruptor: ring buffer is nil")
	}
	if barrier == nil {
		return nil, fmt.Errorf("disruptor: barrier is nil")
	}
	if handler == nil {
		return nil, fmt.Errorf("disruptor: event handler is nil")
	}

	config := defaultProcessorConfig[T]()
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt.applyProcessor(&config); err != nil {
			return nil, fmt.Errorf("applying processor option: %w", err)
		}
	}

	processor := &BatchEventProcessor[T]{
		ringBuffer:       ringBuffer,
		barrier:          barrier,
		handler:          handler,
		exceptionHandler: config.exceptionHandler,
		sequence:         NewSequence(InitialSequenceValue),
	}
	ringBuffer.AddGatingSequences(processor.sequence)

	return processor, nil
}

func (p *BatchEventProcessor[T]) Start(ctx context.Context) error {
	if !p.started.CompareAndSwap(false, true) {
		return ErrClosed
	}

	p.ctx, p.cancel = context.WithCancel(ctx)
	p.sequence.Store(InitialSequenceValue)
	p.wg.Add(1)
	p.processorStateMetric("running", nil)
	go p.run()

	return nil
}

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
	if p.ringBuffer != nil && p.ringBuffer.waitStrategy != nil {
		p.ringBuffer.waitStrategy.SignalAll()
	}
}

func (p *BatchEventProcessor[T]) Wait() error {
	p.wg.Wait()

	if value := p.terminalErr.Load(); value != nil {
		if err, ok := value.(error); ok {
			return err
		}
	}

	return nil
}

func (p *BatchEventProcessor[T]) Sequence() *Sequence {
	return p.sequence
}

func (p *BatchEventProcessor[T]) run() {
	defer p.wg.Done()
	defer p.ringBuffer.RemoveGatingSequence(p.sequence)
	defer p.processorStateMetric("stopped", nil)

	if !p.notifyLifecycleStart() {
		return
	}
	defer p.notifyLifecycleShutdown()

	nextSequence := p.sequence.Value() + 1
	for {
		availableSequence, err := p.barrier.WaitFor(p.ctx, nextSequence)
		if err != nil {
			if p.stopped.Load() || errors.Is(err, context.Canceled) || errors.Is(err, ErrAlerted) {
				p.storeTerminalErr(nil)
				return
			}

			p.storeTerminalErr(err)
			return
		}

		batchRequest := BatchStartRequest{
			Context:    p.ctx,
			BatchSize:  availableSequence - nextSequence + 1,
			QueueDepth: availableSequence - nextSequence + 1,
		}
		if !p.notifyBatchStart(batchRequest) {
			return
		}
		p.batchStartMetric(batchRequest)
		for nextSequence <= availableSequence {
			request := EventRequest[T]{
				Context:    p.ctx,
				Event:      p.ringBuffer.Get(nextSequence),
				Sequence:   nextSequence,
				EndOfBatch: nextSequence == availableSequence,
			}

			var started time.Time
			if p.ringBuffer.metrics != nil {
				started = time.Now()
			}
			err := p.invokeHandler(request)
			p.eventHandledMetric(nextSequence, started, err)
			if err != nil {
				action := p.exceptionHandler.HandleEventException(EventException[T]{
					Context:  p.ctx,
					Event:    request.Event,
					Sequence: nextSequence,
					Err:      err,
				})

				switch action {
				case ExceptionActionContinue:
					p.sequence.Store(nextSequence)
					nextSequence++
					continue
				case ExceptionActionRetry:
					continue
				default:
					p.sequence.Store(nextSequence)
					p.storeTerminalErr(err)
					return
				}
			}

			p.resetRetryState(nextSequence)
			p.sequence.Store(nextSequence)
			nextSequence++
		}
	}
}

func (p *BatchEventProcessor[T]) invokeHandler(request EventRequest[T]) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("disruptor: handler panic: %v", recovered)
		}
	}()

	return p.handler.OnEvent(request)
}

func (p *BatchEventProcessor[T]) invokeBatchStart(
	handler BatchStartHandler,
	request BatchStartRequest,
) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("disruptor: batch start panic: %v", recovered)
		}
	}()

	return handler.OnBatchStart(request)
}

func (p *BatchEventProcessor[T]) invokeLifecycleStart(
	handler LifecycleHandler,
) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("disruptor: lifecycle start panic: %v", recovered)
		}
	}()

	return handler.OnStart(p.ctx)
}

func (p *BatchEventProcessor[T]) invokeLifecycleShutdown(
	handler LifecycleHandler,
) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("disruptor: lifecycle shutdown panic: %v", recovered)
		}
	}()

	return handler.OnShutdown(p.ctx)
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

func (p *BatchEventProcessor[T]) notifyLifecycleStart() bool {
	handler, ok := p.handler.(LifecycleHandler)
	if !ok {
		return true
	}

	for {
		err := p.invokeLifecycleStart(handler)
		if err == nil {
			return true
		}

		action := p.exceptionHandler.HandleStartException(LifecycleException{
			Context: p.ctx,
			Err:     err,
		})
		switch action {
		case ExceptionActionContinue:
			return true
		case ExceptionActionRetry:
			continue
		default:
			p.storeTerminalErr(err)
			return false
		}
	}
}

func (p *BatchEventProcessor[T]) notifyLifecycleShutdown() {
	handler, ok := p.handler.(LifecycleHandler)
	if !ok {
		return
	}

	for {
		err := p.invokeLifecycleShutdown(handler)
		if err == nil {
			return
		}

		action := p.exceptionHandler.HandleShutdownException(LifecycleException{
			Context: p.ctx,
			Err:     err,
		})
		switch action {
		case ExceptionActionContinue:
			return
		case ExceptionActionRetry:
			continue
		default:
			p.storeTerminalErr(err)
			return
		}
	}
}

func (p *BatchEventProcessor[T]) notifyBatchStart(request BatchStartRequest) bool {
	handler, ok := p.handler.(BatchStartHandler)
	if !ok {
		return true
	}

	for {
		err := p.invokeBatchStart(handler, request)
		if err == nil {
			return true
		}

		action := p.exceptionHandler.HandleStartException(LifecycleException{
			Context: request.Context,
			Err:     err,
		})
		switch action {
		case ExceptionActionContinue:
			return true
		case ExceptionActionRetry:
			continue
		default:
			p.storeTerminalErr(err)
			return false
		}
	}
}

func (p *BatchEventProcessor[T]) batchStartMetric(request BatchStartRequest) {
	if p.ringBuffer.metrics == nil {
		return
	}

	p.ringBuffer.metrics.OnBatchStart(BatchMetric{
		BatchSize:  request.BatchSize,
		QueueDepth: request.QueueDepth,
	})
}

func (p *BatchEventProcessor[T]) eventHandledMetric(sequence int64, started time.Time, err error) {
	if p.ringBuffer.metrics == nil {
		return
	}

	p.ringBuffer.metrics.OnEventHandled(EventMetric{
		Sequence: sequence,
		Duration: time.Since(started),
		Err:      err,
	})
}

func (p *BatchEventProcessor[T]) processorStateMetric(state string, err error) {
	if p.ringBuffer.metrics == nil {
		return
	}

	p.ringBuffer.metrics.OnProcessorState(ProcessorMetric{
		State: state,
		Err:   err,
	})
}

func (p *BatchEventProcessor[T]) resetRetryState(sequence int64) {
	resetter, ok := p.exceptionHandler.(interface{ resetRetry(sequence int64) })
	if !ok {
		return
	}

	resetter.resetRetry(sequence)
}
