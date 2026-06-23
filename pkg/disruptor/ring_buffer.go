package disruptor

import (
	"context"
	"fmt"
	"sync"
)

type RingBuffer[T any] struct {
	mu sync.Mutex

	entries         []T
	mask            int64
	size            int64
	nextSequence    int64
	cursor          *Sequence
	gatingSequences []*Sequence
	available       map[int64]struct{}
	waitStrategy    WaitStrategy
	producerType    ProducerType
	metrics         MetricsSink
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
		entries:         entries,
		mask:            int64(size - 1),
		size:            int64(size),
		nextSequence:    InitialSequenceValue,
		cursor:          NewSequence(InitialSequenceValue),
		gatingSequences: []*Sequence{},
		available:       map[int64]struct{}{},
		waitStrategy:    config.waitStrategy,
		producerType:    config.producerType,
		metrics:         config.metrics,
	}, nil
}

func (r *RingBuffer[T]) Next(ctx context.Context) (int64, error) {
	return r.NextN(ctx, 1)
}

func (r *RingBuffer[T]) NextN(ctx context.Context, n int64) (int64, error) {
	if n <= 0 || n > r.size {
		return InitialSequenceValue, ErrInvalidSequence
	}

	for {
		if err := ctx.Err(); err != nil {
			return InitialSequenceValue, err
		}

		r.mu.Lock()
		nextSequence := r.nextSequence + n
		if r.hasAvailableCapacityLocked(nextSequence) {
			r.nextSequence = nextSequence
			r.mu.Unlock()

			return nextSequence, nil
		}

		request := r.capacityWaitRequestLocked(ctx, n, nextSequence)
		r.mu.Unlock()

		if err := r.waitStrategy.WaitForCapacity(request); err != nil {
			return InitialSequenceValue, err
		}
	}
}

func (r *RingBuffer[T]) TryNext() (int64, error) {
	return r.TryNextN(1)
}

func (r *RingBuffer[T]) TryNextN(n int64) (int64, error) {
	if n <= 0 || n > r.size {
		return InitialSequenceValue, ErrInvalidSequence
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	nextSequence := r.nextSequence + n
	if !r.hasAvailableCapacityLocked(nextSequence) {
		return InitialSequenceValue, ErrInsufficientCapacity
	}

	r.nextSequence = nextSequence
	return nextSequence, nil
}

func (r *RingBuffer[T]) Get(sequence int64) *T {
	return &r.entries[sequence&r.mask]
}

func (r *RingBuffer[T]) Publish(sequence int64) {
	r.PublishRange(sequence, sequence)
}

func (r *RingBuffer[T]) PublishRange(lo, hi int64) {
	if lo > hi {
		return
	}

	r.mu.Lock()
	for sequence := lo; sequence <= hi; sequence++ {
		r.available[sequence] = struct{}{}
	}
	r.advanceCursorLocked()
	r.mu.Unlock()

	r.waitStrategy.SignalAll()
	r.publishMetric(lo, hi, nil)
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

	defer r.Publish(sequence)
	translator.Translate(TranslateRequest[T]{
		Context:  ctx,
		Event:    r.Get(sequence),
		Sequence: sequence,
	})

	return nil
}

func (r *RingBuffer[T]) AddGatingSequences(sequences ...*Sequence) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, sequence := range sequences {
		if sequence == nil {
			continue
		}
		r.gatingSequences = append(r.gatingSequences, sequence)
	}
}

func (r *RingBuffer[T]) RemoveGatingSequence(sequence *Sequence) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i, item := range r.gatingSequences {
		if item != sequence {
			continue
		}

		r.gatingSequences = append(
			r.gatingSequences[:i],
			r.gatingSequences[i+1:]...,
		)

		return true
	}

	return false
}

func (r *RingBuffer[T]) NewBarrier(dependencies ...*Sequence) Barrier {
	return newProcessingBarrier(r.cursor, r.waitStrategy, dependencies...)
}

func (r *RingBuffer[T]) hasAvailableCapacityLocked(nextSequence int64) bool {
	if len(r.gatingSequences) == 0 {
		return true
	}

	wrapPoint := nextSequence - r.size
	return wrapPoint <= r.minimumGatingSequenceLocked()
}

func (r *RingBuffer[T]) minimumGatingSequenceLocked() int64 {
	minimum := r.gatingSequences[0].Value()
	for _, sequence := range r.gatingSequences[1:] {
		value := sequence.Value()
		if value < minimum {
			minimum = value
		}
	}

	return minimum
}

func (r *RingBuffer[T]) capacityWaitRequestLocked(
	ctx context.Context,
	requestedSequences int64,
	nextSequence int64,
) CapacityWaitRequest {
	var gating SequenceReader
	if len(r.gatingSequences) > 0 {
		gating = r.gatingSequences[0]
	}

	return CapacityWaitRequest{
		Context:            ctx,
		RequestedSequences: requestedSequences,
		CurrentSequence:    r.nextSequence,
		WrapPoint:          nextSequence - r.size,
		GatingSequence:     gating,
	}
}

func (r *RingBuffer[T]) advanceCursorLocked() {
	for sequence := r.cursor.Value() + 1; ; sequence++ {
		if _, ok := r.available[sequence]; !ok {
			return
		}

		delete(r.available, sequence)
		r.cursor.Store(sequence)
	}
}

func (r *RingBuffer[T]) publishMetric(lo, hi int64, err error) {
	if r.metrics == nil {
		return
	}

	r.metrics.OnPublish(PublishMetric{
		ProducerType: r.producerType,
		Sequence:     hi,
		BatchSize:    hi - lo + 1,
		Err:          err,
	})
}
