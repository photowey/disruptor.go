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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

// Disruptor coordinates a ring buffer and a set of event processors.
type Disruptor[T any] struct {
	mu sync.Mutex

	ringBuffer *RingBuffer[T]
	processors []EventProcessor
	mode       consumerMode
	started    atomic.Bool
}

type consumerMode uint8

const (
	consumerModeUnset consumerMode = iota
	consumerModeFanOut
	consumerModeGraph
)

// New creates a high-level Disruptor facade backed by a ring buffer.
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

// RingBuffer returns the underlying ring buffer.
func (d *Disruptor[T]) RingBuffer() *RingBuffer[T] {
	return d.ringBuffer
}

// HandleEventsWith registers handlers that each receive every event.
func (d *Disruptor[T]) HandleEventsWith(
	handlers ...EventHandler[T],
) ([]EventProcessor, error) {
	return d.HandleEventsWithOptions(handlers)
}

// HandleEventsWithOptions registers handlers with processor-level options.
func (d *Disruptor[T]) HandleEventsWithOptions(
	handlers []EventHandler[T],
	opts ...ProcessorOption[T],
) ([]EventProcessor, error) {
	if len(handlers) == 0 {
		return nil, fmt.Errorf("disruptor: no event handlers")
	}
	for _, handler := range handlers {
		if handler == nil {
			return nil, fmt.Errorf("disruptor: event handler is nil")
		}
	}

	processorOptions := processorConfig[T]{
		exceptionHandler: defaultProcessorConfig[T]().exceptionHandler,
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt.applyProcessor(&processorOptions); err != nil {
			return nil, fmt.Errorf("applying processor option: %w", err)
		}
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if d.started.Load() {
		return nil, ErrClosed
	}
	if d.mode == consumerModeGraph {
		return nil, fmt.Errorf(
			"%w: HandleEventsWith cannot be used after HandleGraph",
			ErrConsumerModeConflict,
		)
	}

	processors := make([]EventProcessor, 0, len(handlers))
	for _, handler := range handlers {
		processor, err := newBatchEventProcessor(
			d.ringBuffer,
			d.ringBuffer.NewBarrier(),
			handler,
			batchEventProcessorConfig[T]{
				exceptionHandler: processorOptions.exceptionHandler,
				producerGating:   true,
				haltAdvances:     true,
			},
		)
		if err != nil {
			for _, created := range processors {
				if batchProcessor, ok := created.(*BatchEventProcessor[T]); ok {
					batchProcessor.removeGatingSequence()
				}
			}
			return nil, fmt.Errorf("creating batch event processor: %w", err)
		}

		processors = append(processors, processor)
	}
	d.mode = consumerModeFanOut
	d.processors = append(d.processors, processors...)

	return processors, nil
}

// Start starts all registered event processors.
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

// Stop requests all registered processors to stop.
func (d *Disruptor[T]) Stop() {
	d.mu.Lock()
	processors := append([]EventProcessor(nil), d.processors...)
	d.mu.Unlock()

	for _, processor := range processors {
		processor.Stop()
	}
}

// Wait waits for all processors and joins their terminal errors.
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
