package sequencer

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
)

type singleProducerSequencer struct {
	gatingMu sync.RWMutex

	size            int64
	nextSequence    int64
	cursor          *Sequence
	gatingSequences []*Sequence
	waitStrategy    CapacityWaitStrategy

	gatingCount          atomic.Int64
	cachedGatingSequence atomic.Int64
}

func NewSingleProducer(size int64, waitStrategy CapacityWaitStrategy) Sequencer {
	sequencer := &singleProducerSequencer{
		size:            size,
		nextSequence:    InitialSequenceValue,
		cursor:          NewSequence(InitialSequenceValue),
		gatingSequences: []*Sequence{},
		waitStrategy:    waitStrategy,
	}
	sequencer.cachedGatingSequence.Store(InitialSequenceValue)

	return sequencer
}

func (s *singleProducerSequencer) Next(ctx context.Context) (int64, error) {
	return s.NextN(ctx, 1)
}

func (s *singleProducerSequencer) NextN(
	ctx context.Context,
	n int64,
) (int64, error) {
	if n <= 0 || n > s.size {
		return InitialSequenceValue, ErrInvalidSequence
	}

	for {
		if err := ctx.Err(); err != nil {
			return InitialSequenceValue, err
		}

		nextSequence := s.nextSequence + n
		if s.hasAvailableCapacity(nextSequence) {
			s.nextSequence = nextSequence
			return nextSequence, nil
		}

		request := s.capacityWaitRequest(ctx, n, nextSequence)
		if s.waitStrategy == nil {
			runtime.Gosched()
			continue
		}
		if err := s.waitStrategy.WaitForCapacity(request); err != nil {
			return InitialSequenceValue, err
		}
	}
}

func (s *singleProducerSequencer) TryNext() (int64, error) {
	return s.TryNextN(1)
}

func (s *singleProducerSequencer) TryNextN(n int64) (int64, error) {
	if n <= 0 || n > s.size {
		return InitialSequenceValue, ErrInvalidSequence
	}

	nextSequence := s.nextSequence + n
	if !s.hasAvailableCapacity(nextSequence) {
		return InitialSequenceValue, ErrInsufficientCapacity
	}

	s.nextSequence = nextSequence
	return nextSequence, nil
}

func (s *singleProducerSequencer) Publish(sequence int64) {
	s.PublishRange(sequence, sequence)
}

func (s *singleProducerSequencer) PublishRange(lo, hi int64) {
	if lo > hi {
		return
	}

	s.cursor.Store(hi)
}

func (s *singleProducerSequencer) Cursor() *Sequence {
	return s.cursor
}

func (s *singleProducerSequencer) AddGatingSequences(sequences ...*Sequence) {
	s.gatingMu.Lock()
	defer s.gatingMu.Unlock()

	added := 0
	for _, sequence := range sequences {
		if sequence != nil {
			added++
		}
	}
	if added == 0 {
		return
	}

	s.gatingCount.Store(int64(len(s.gatingSequences) + added))
	for _, sequence := range sequences {
		if sequence == nil {
			continue
		}
		s.gatingSequences = append(s.gatingSequences, sequence)
	}

	s.cachedGatingSequence.Store(InitialSequenceValue)
}

func (s *singleProducerSequencer) RemoveGatingSequence(sequence *Sequence) bool {
	s.gatingMu.Lock()
	defer s.gatingMu.Unlock()

	for i, item := range s.gatingSequences {
		if item != sequence {
			continue
		}

		s.gatingSequences = append(
			s.gatingSequences[:i],
			s.gatingSequences[i+1:]...,
		)
		s.gatingCount.Store(int64(len(s.gatingSequences)))
		s.cachedGatingSequence.Store(InitialSequenceValue)

		return true
	}

	return false
}

func (s *singleProducerSequencer) HighestPublished(
	lowerBound int64,
	availableSequence int64,
) int64 {
	cursor := s.cursor.Value()
	if cursor < lowerBound {
		return lowerBound - 1
	}
	if cursor < availableSequence {
		return cursor
	}

	return availableSequence
}

func (s *singleProducerSequencer) Available(sequence int64) bool {
	return sequence <= s.cursor.Value()
}

func (s *singleProducerSequencer) hasAvailableCapacity(
	nextSequence int64,
) bool {
	if s.gatingCount.Load() == 0 {
		return true
	}

	wrapPoint := nextSequence - s.size
	cachedGatingSequence := s.cachedGatingSequence.Load()
	if wrapPoint <= cachedGatingSequence {
		return true
	}

	minimumGatingSequence := s.minimumGatingSequence()
	s.cachedGatingSequence.Store(minimumGatingSequence)

	return wrapPoint <= minimumGatingSequence
}

func (s *singleProducerSequencer) minimumGatingSequence() int64 {
	s.gatingMu.RLock()
	defer s.gatingMu.RUnlock()

	if len(s.gatingSequences) == 0 {
		return s.nextSequence
	}

	minimum := s.gatingSequences[0].Value()
	for _, sequence := range s.gatingSequences[1:] {
		value := sequence.Value()
		if value < minimum {
			minimum = value
		}
	}

	return minimum
}

func (s *singleProducerSequencer) capacityWaitRequest(
	ctx context.Context,
	requestedSequences int64,
	nextSequence int64,
) CapacityWaitRequest {
	return CapacityWaitRequest{
		Context:            ctx,
		RequestedSequences: requestedSequences,
		CurrentSequence:    s.nextSequence,
		WrapPoint:          nextSequence - s.size,
		GatingSequence:     s.gatingSequenceReader(),
	}
}

func (s *singleProducerSequencer) gatingSequenceReader() SequenceReader {
	return singleProducerGatingReader{sequencer: s}
}
