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

	"github.com/photowey/disruptor.go/pkg/event"
	"github.com/photowey/disruptor.go/pkg/processor"
	"github.com/photowey/disruptor.go/pkg/ringbuffer"
)

// Disruptor coordinates a ring buffer and a set of event processors.
type Disruptor[T any] struct {
	mu sync.Mutex

	ringBuffer *ringbuffer.RingBuffer[T]
	processors []processor.EventProcessor
	mode       consumerMode
	started    atomic.Bool
}

type consumerMode uint8

const (
	consumerModeUnset consumerMode = iota
	consumerModeFanOut
	consumerModeGraph
)

type processorStopper interface {
	Stop()
}

type processorWaitTask struct {
	processor processor.EventProcessor
	stopOnce  *sync.Once
	stopper   processorStopper
	errc      chan<- error
}

func (task processorWaitTask) run() {
	err := task.processor.Wait()
	if err != nil {
		task.stopOnce.Do(task.stopper.Stop)
	}

	task.errc <- err
}

// New creates a high-level Disruptor facade backed by a ring buffer.
func New[T any](
	factory event.Factory[T],
	size int,
	opts ...ringbuffer.Option,
) (*Disruptor[T], error) {
	ringBuffer, err := ringbuffer.New(factory, size, opts...)
	if err != nil {
		return nil, err
	}

	return &Disruptor[T]{
		ringBuffer: ringBuffer,
		processors: []processor.EventProcessor{},
	}, nil
}

// RingBuffer returns the underlying ring buffer.
func (d *Disruptor[T]) RingBuffer() *ringbuffer.RingBuffer[T] {
	return d.ringBuffer
}

// HandleEventsWith registers handlers that each receive every event.
func (d *Disruptor[T]) HandleEventsWith(
	handlers ...event.Handler[T],
) ([]processor.EventProcessor, error) {
	return d.HandleEventsWithOptions(handlers)
}

// HandleEventsWithOptions registers handlers with processor-level options.
func (d *Disruptor[T]) HandleEventsWithOptions(
	handlers []event.Handler[T],
	opts ...processor.Option[T],
) ([]processor.EventProcessor, error) {
	if len(handlers) == 0 {
		return nil, fmt.Errorf("disruptor: no event handlers")
	}
	for _, handler := range handlers {
		if handler == nil {
			return nil, fmt.Errorf("disruptor: event handler is nil")
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

	processors := make([]processor.EventProcessor, 0, len(handlers))
	for _, handler := range handlers {
		eventProcessor, err := processor.NewBatchEventProcessor(
			d.ringBuffer,
			d.ringBuffer.NewBarrier(),
			handler,
			opts...,
		)
		if err != nil {
			for _, created := range processors {
				if batchProcessor, ok := created.(*processor.BatchEventProcessor[T]); ok {
					batchProcessor.RemoveGatingSequence()
				}
			}
			return nil, fmt.Errorf("creating batch event processor: %w", err)
		}

		processors = append(processors, eventProcessor)
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
	processors := append([]processor.EventProcessor(nil), d.processors...)
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
	processors := append([]processor.EventProcessor(nil), d.processors...)
	d.mu.Unlock()

	for _, processor := range processors {
		processor.Stop()
	}
}

// Wait waits for all processors and joins their terminal errors.
func (d *Disruptor[T]) Wait() error {
	d.mu.Lock()
	processors := append([]processor.EventProcessor(nil), d.processors...)
	d.mu.Unlock()
	if len(processors) == 0 {
		return nil
	}

	var stopOnce sync.Once
	errc := make(chan error, len(processors))
	for _, processor := range processors {
		task := processorWaitTask{
			processor: processor,
			stopOnce:  &stopOnce,
			stopper:   d,
			errc:      errc,
		}
		go task.run()
	}

	errs := make([]error, 0, len(processors))
	for range processors {
		if err := <-errc; err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}
