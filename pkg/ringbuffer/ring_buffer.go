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

package ringbuffer

import (
	"context"
	"fmt"
	"time"

	internalsequencer "github.com/photowey/disruptor.go/internal/sequencer"
	"github.com/photowey/disruptor.go/pkg/event"
	"github.com/photowey/disruptor.go/pkg/metrics"
	"github.com/photowey/disruptor.go/pkg/sequence"
	"github.com/photowey/disruptor.go/pkg/wait"
)

// RingBuffer stores preallocated events and coordinates producer publication.
type RingBuffer[T any] struct {
	entries      []T
	mask         int64
	sequencer    internalsequencer.Sequencer
	waitStrategy wait.Strategy
	producerType ProducerType
	metrics      metrics.Sink
}

// New creates a ring buffer with a power-of-two size.
func New[T any](
	factory event.Factory[T],
	size int,
	opts ...Option,
) (*RingBuffer[T], error) {
	if size <= 0 || size&(size-1) != 0 {
		return nil, ErrInvalidBufferSize
	}
	if factory == nil {
		return nil, fmt.Errorf("ringbuffer: event factory is nil")
	}

	config := defaultOptions()
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(&config); err != nil {
			return nil, fmt.Errorf("applying ring buffer option: %w", err)
		}
	}

	entries := make([]T, size)
	for i := range entries {
		entries[i] = factory.NewEvent()
	}

	return &RingBuffer[T]{
		entries:      entries,
		mask:         int64(size - 1),
		sequencer:    newSequencer(int64(size), config),
		waitStrategy: config.waitStrategy,
		producerType: config.producerType,
		metrics:      config.metrics,
	}, nil
}

// Next claims one sequence, blocking until capacity is available or ctx ends.
func (r *RingBuffer[T]) Next(ctx context.Context) (int64, error) {
	return r.NextN(ctx, 1)
}

// NextN claims n sequences and returns the highest claimed sequence.
func (r *RingBuffer[T]) NextN(ctx context.Context, n int64) (int64, error) {
	return r.sequencer.NextN(ctx, n)
}

// TryNext claims one sequence without blocking.
func (r *RingBuffer[T]) TryNext() (int64, error) {
	return r.TryNextN(1)
}

// TryNextN claims n sequences without blocking.
func (r *RingBuffer[T]) TryNextN(n int64) (int64, error) {
	return r.sequencer.TryNextN(n)
}

// Get returns the mutable event slot for sequence.
func (r *RingBuffer[T]) Get(sequenceValue int64) *T {
	return &r.entries[sequenceValue&r.mask]
}

// Publish marks a single claimed sequence as available.
func (r *RingBuffer[T]) Publish(sequenceValue int64) {
	r.publishRange(sequenceValue, sequenceValue, time.Time{}, nil)
}

// PublishRange marks an inclusive sequence range as available.
func (r *RingBuffer[T]) PublishRange(lo int64, hi int64) {
	r.publishRange(lo, hi, time.Time{}, nil)
}

func (r *RingBuffer[T]) publishRange(lo int64, hi int64, started time.Time, err error) {
	if lo > hi {
		return
	}

	r.sequencer.PublishRange(lo, hi)
	r.waitStrategy.SignalAll()
	r.publishMetric(lo, hi, started, err)
}

// PublishEvent claims, translates, and publishes one event.
func (r *RingBuffer[T]) PublishEvent(
	ctx context.Context,
	translator event.Translator[T],
) error {
	if translator == nil {
		return fmt.Errorf("ringbuffer: event translator is nil")
	}

	sequenceValue, err := r.Next(ctx)
	if err != nil {
		return err
	}

	var started time.Time
	if r.metrics != nil {
		started = time.Now()
	}
	defer r.publishRange(sequenceValue, sequenceValue, started, nil)
	translator.Translate(event.TranslateRequest[T]{
		Context:  ctx,
		Event:    r.Get(sequenceValue),
		Sequence: sequenceValue,
	})

	return nil
}

// AddGatingSequences registers consumer sequences for producer backpressure.
func (r *RingBuffer[T]) AddGatingSequences(sequences ...*sequence.Sequence) {
	r.sequencer.AddGatingSequences(sequences...)
}

// RemoveGatingSequence unregisters a consumer sequence.
func (r *RingBuffer[T]) RemoveGatingSequence(sequenceValue *sequence.Sequence) bool {
	removed := r.sequencer.RemoveGatingSequence(sequenceValue)
	if removed {
		r.waitStrategy.SignalAll()
	}

	return removed
}

// NewBarrier creates a processing barrier over the ring buffer cursor.
func (r *RingBuffer[T]) NewBarrier(dependencies ...*sequence.Sequence) Barrier {
	return newProcessingBarrier(r.sequencer.Cursor(), r.waitStrategy, r.metrics, dependencies...)
}

// SignalAll wakes waiters blocked by the configured wait strategy.
func (r *RingBuffer[T]) SignalAll() {
	r.waitStrategy.SignalAll()
}

// Metrics returns the configured metrics sink.
func (r *RingBuffer[T]) Metrics() metrics.Sink {
	return r.metrics
}

func (r *RingBuffer[T]) publishMetric(lo int64, hi int64, started time.Time, err error) {
	if r.metrics == nil {
		return
	}

	var duration time.Duration
	if !started.IsZero() {
		duration = time.Since(started)
	}

	r.metrics.OnPublish(metrics.PublishMetric{
		ProducerType: producerTypeName(r.producerType),
		Sequence:     hi,
		BatchSize:    hi - lo + 1,
		Duration:     duration,
		Err:          err,
	})
}

func newSequencer(
	size int64,
	config options,
) internalsequencer.Sequencer {
	if config.producerType == ProducerTypeSingle {
		return internalsequencer.NewSingleProducer(size, config.waitStrategy)
	}

	return internalsequencer.NewMultiProducer(size, config.waitStrategy)
}
