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
	"sync/atomic"
	"time"
)

// Barrier waits for available sequences and supports cooperative alerts.
type Barrier interface {
	WaitFor(ctx context.Context, sequence int64) (int64, error)
	Cursor() int64
	Alert()
	ClearAlert()
	CheckAlert() error
	Alerted() bool
}

type processingBarrier struct {
	cursor            *Sequence
	dependencies      []*Sequence
	dependentSequence SequenceReader
	waitStrategy      WaitStrategy
	metrics           MetricsSink
	alerted           atomic.Bool
}

func newProcessingBarrier(
	cursor *Sequence,
	waitStrategy WaitStrategy,
	metrics MetricsSink,
	dependencies ...*Sequence,
) *processingBarrier {
	copiedDependencies := append([]*Sequence(nil), dependencies...)

	return &processingBarrier{
		cursor:            cursor,
		dependencies:      copiedDependencies,
		dependentSequence: newMinimumSequenceReader(copiedDependencies),
		waitStrategy:      waitStrategy,
		metrics:           metrics,
	}
}

func (b *processingBarrier) WaitFor(ctx context.Context, sequence int64) (int64, error) {
	for {
		if err := ctx.Err(); err != nil {
			return InitialSequenceValue, err
		}
		if err := b.CheckAlert(); err != nil {
			return InitialSequenceValue, err
		}

		available := b.availableSequence()
		if available >= sequence {
			return available, nil
		}

		var started time.Time
		if b.metrics != nil {
			started = time.Now()
		}
		available, err := b.waitStrategy.WaitFor(WaitRequest{
			Context:           ctx,
			RequestedSequence: sequence,
			CursorSequence:    b.cursor,
			DependentSequence: b.dependentSequence,
			Barrier:           b,
		})
		b.waitMetric(sequence, available, started, err)
		if err != nil {
			return InitialSequenceValue, err
		}
	}
}

func (b *processingBarrier) Cursor() int64 {
	return b.availableSequence()
}

func (b *processingBarrier) Alert() {
	b.alerted.Store(true)
	b.waitStrategy.SignalAll()
}

func (b *processingBarrier) ClearAlert() {
	b.alerted.Store(false)
}

func (b *processingBarrier) CheckAlert() error {
	if b.Alerted() {
		return ErrAlerted
	}

	return nil
}

func (b *processingBarrier) Alerted() bool {
	return b.alerted.Load()
}

func (b *processingBarrier) availableSequence() int64 {
	available := b.cursor.Value()
	for _, dependency := range b.dependencies {
		if dependency == nil {
			continue
		}

		value := dependency.Value()
		if value < available {
			available = value
		}
	}

	return available
}

func (b *processingBarrier) waitMetric(
	requestedSequence int64,
	availableSequence int64,
	started time.Time,
	err error,
) {
	if b.metrics == nil {
		return
	}

	b.metrics.OnWait(WaitMetric{
		RequestedSequence: requestedSequence,
		AvailableSequence: availableSequence,
		Duration:          time.Since(started),
		Strategy:          "wait",
		Err:               err,
	})
}
