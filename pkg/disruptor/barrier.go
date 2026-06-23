package disruptor

import (
	"context"
	"sync/atomic"
	"time"
)

type Barrier interface {
	WaitFor(ctx context.Context, sequence int64) (int64, error)
	Cursor() int64
	Alert()
	ClearAlert()
	CheckAlert() error
	Alerted() bool
}

type processingBarrier struct {
	cursor       *Sequence
	dependencies []*Sequence
	waitStrategy WaitStrategy
	metrics      MetricsSink
	alerted      atomic.Bool
}

func newProcessingBarrier(
	cursor *Sequence,
	waitStrategy WaitStrategy,
	metrics MetricsSink,
	dependencies ...*Sequence,
) *processingBarrier {
	return &processingBarrier{
		cursor:       cursor,
		dependencies: dependencies,
		waitStrategy: waitStrategy,
		metrics:      metrics,
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
