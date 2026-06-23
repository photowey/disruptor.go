package disruptor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
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

		for nextSequence <= availableSequence {
			request := EventRequest[T]{
				Context:    p.ctx,
				Event:      p.ringBuffer.Get(nextSequence),
				Sequence:   nextSequence,
				EndOfBatch: nextSequence == availableSequence,
			}

			err := p.invokeHandler(request)
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

func (p *BatchEventProcessor[T]) storeTerminalErr(err error) {
	if err == nil {
		return
	}

	p.terminalErr.Store(err)
}
