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
	"time"

	internalsequencer "github.com/photowey/disruptor.go/internal/sequencer"
)

type RingBuffer[T any] struct {
	entries      []T
	mask         int64
	sequencer    internalsequencer.Sequencer
	waitStrategy WaitStrategy
	producerType ProducerType
	metrics      MetricsSink
}

func NewRingBuffer[T any](
	factory EventFactory[T],
	size int,
	opts ...RingBufferOption,
) (*RingBuffer[T], error) {
	if size <= 0 || size&(size-1) != 0 {
		return nil, ErrInvalidBufferSize
	}
	if factory == nil {
		return nil, fmt.Errorf("disruptor: event factory is nil")
	}

	config := defaultRingBufferConfig()
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt.applyRingBuffer(&config); err != nil {
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
		sequencer:    newRingBufferSequencer(int64(size), config),
		waitStrategy: config.waitStrategy,
		producerType: config.producerType,
		metrics:      config.metrics,
	}, nil
}

func (r *RingBuffer[T]) Next(ctx context.Context) (int64, error) {
	return r.NextN(ctx, 1)
}

func (r *RingBuffer[T]) NextN(ctx context.Context, n int64) (int64, error) {
	return r.sequencer.NextN(ctx, n)
}

func (r *RingBuffer[T]) TryNext() (int64, error) {
	return r.TryNextN(1)
}

func (r *RingBuffer[T]) TryNextN(n int64) (int64, error) {
	return r.sequencer.TryNextN(n)
}

func (r *RingBuffer[T]) Get(sequence int64) *T {
	return &r.entries[sequence&r.mask]
}

func (r *RingBuffer[T]) Publish(sequence int64) {
	r.publishRange(sequence, sequence, time.Time{}, nil)
}

func (r *RingBuffer[T]) PublishRange(lo, hi int64) {
	r.publishRange(lo, hi, time.Time{}, nil)
}

func (r *RingBuffer[T]) publishRange(lo, hi int64, started time.Time, err error) {
	if lo > hi {
		return
	}

	r.sequencer.PublishRange(lo, hi)
	r.waitStrategy.SignalAll()
	r.publishMetric(lo, hi, started, err)
}

func (r *RingBuffer[T]) PublishEvent(
	ctx context.Context,
	translator EventTranslator[T],
) error {
	if translator == nil {
		return fmt.Errorf("disruptor: event translator is nil")
	}

	sequence, err := r.Next(ctx)
	if err != nil {
		return err
	}

	var started time.Time
	if r.metrics != nil {
		started = time.Now()
	}
	defer r.publishRange(sequence, sequence, started, nil)
	translator.Translate(TranslateRequest[T]{
		Context:  ctx,
		Event:    r.Get(sequence),
		Sequence: sequence,
	})

	return nil
}

func (r *RingBuffer[T]) AddGatingSequences(sequences ...*Sequence) {
	r.sequencer.AddGatingSequences(sequences...)
}

func (r *RingBuffer[T]) RemoveGatingSequence(sequence *Sequence) bool {
	removed := r.sequencer.RemoveGatingSequence(sequence)
	if removed {
		r.waitStrategy.SignalAll()
	}

	return removed
}

func (r *RingBuffer[T]) NewBarrier(dependencies ...*Sequence) Barrier {
	return newProcessingBarrier(r.sequencer.Cursor(), r.waitStrategy, r.metrics, dependencies...)
}

func (r *RingBuffer[T]) publishMetric(lo, hi int64, started time.Time, err error) {
	if r.metrics == nil {
		return
	}

	var duration time.Duration
	if !started.IsZero() {
		duration = time.Since(started)
	}

	r.metrics.OnPublish(PublishMetric{
		ProducerType: r.producerType,
		Sequence:     hi,
		BatchSize:    hi - lo + 1,
		Duration:     duration,
		Err:          err,
	})
}

func newRingBufferSequencer(
	size int64,
	config ringBufferConfig,
) internalsequencer.Sequencer {
	if config.producerType == ProducerTypeSingle {
		return internalsequencer.NewSingleProducer(size, config.waitStrategy)
	}

	return internalsequencer.NewMultiProducer(size, config.waitStrategy)
}
