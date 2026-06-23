package disruptor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

type Disruptor[T any] struct {
	mu sync.Mutex

	ringBuffer *RingBuffer[T]
	processors []EventProcessor
	started    atomic.Bool
}

func New[T any](
	factory EventFactory[T],
	size int,
	opts ...RingBufferOption,
) (*Disruptor[T], error) {
	ringBuffer, err := NewRingBuffer(factory, size, opts...)
	if err != nil {
		return nil, err
	}

	return &Disruptor[T]{
		ringBuffer: ringBuffer,
		processors: []EventProcessor{},
	}, nil
}

func (d *Disruptor[T]) RingBuffer() *RingBuffer[T] {
	return d.ringBuffer
}

func (d *Disruptor[T]) HandleEventsWith(
	handlers ...EventHandler[T],
) ([]EventProcessor, error) {
	return d.HandleEventsWithOptions(handlers)
}

func (d *Disruptor[T]) HandleEventsWithOptions(
	handlers []EventHandler[T],
	opts ...ProcessorOption[T],
) ([]EventProcessor, error) {
	if len(handlers) == 0 {
		return nil, fmt.Errorf("disruptor: no event handlers")
	}
	if d.started.Load() {
		return nil, ErrClosed
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	processors := make([]EventProcessor, 0, len(handlers))
	for _, handler := range handlers {
		if handler == nil {
			return nil, fmt.Errorf("disruptor: event handler is nil")
		}

		processor, err := NewBatchEventProcessor(
			d.ringBuffer,
			d.ringBuffer.NewBarrier(),
			handler,
			opts...,
		)
		if err != nil {
			return nil, fmt.Errorf("creating batch event processor: %w", err)
		}

		processors = append(processors, processor)
		d.processors = append(d.processors, processor)
	}

	return processors, nil
}

func (d *Disruptor[T]) Start(ctx context.Context) error {
	if !d.started.CompareAndSwap(false, true) {
		return ErrClosed
	}

	d.mu.Lock()
	processors := append([]EventProcessor(nil), d.processors...)
	d.mu.Unlock()

	for _, processor := range processors {
		if err := processor.Start(ctx); err != nil {
			d.Stop()
			return fmt.Errorf("starting event processor: %w", err)
		}
	}

	return nil
}

func (d *Disruptor[T]) Stop() {
	d.mu.Lock()
	processors := append([]EventProcessor(nil), d.processors...)
	d.mu.Unlock()

	for _, processor := range processors {
		processor.Stop()
	}
}

func (d *Disruptor[T]) Wait() error {
	d.mu.Lock()
	processors := append([]EventProcessor(nil), d.processors...)
	d.mu.Unlock()
	if len(processors) == 0 {
		return nil
	}

	var stopOnce sync.Once
	errc := make(chan error, len(processors))
	for _, processor := range processors {
		go func(processor EventProcessor) {
			err := processor.Wait()
			if err != nil {
				stopOnce.Do(d.Stop)
			}

			errc <- err
		}(processor)
	}

	errs := make([]error, 0, len(processors))
	for range processors {
		if err := <-errc; err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}
