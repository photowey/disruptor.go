package sequencer

import (
	"context"
	"runtime"
	"sync"

	"github.com/photowey/disruptor.go/internal/availability"
)

type baseSequencer struct {
	mu sync.Mutex

	size            int64
	nextSequence    int64
	cursor          *Sequence
	gatingSequences []*Sequence
	available       map[int64]struct{}
	scanner         availability.Scanner
	waitStrategy    CapacityWaitStrategy
}

func newBaseSequencer(size int64, waitStrategy CapacityWaitStrategy) *baseSequencer {
	return &baseSequencer{
		size:            size,
		nextSequence:    InitialSequenceValue,
		cursor:          NewSequence(InitialSequenceValue),
		gatingSequences: []*Sequence{},
		available:       map[int64]struct{}{},
		scanner:         availability.NewScalarScanner(),
		waitStrategy:    waitStrategy,
	}
}

func (s *baseSequencer) Next(ctx context.Context) (int64, error) {
	return s.NextN(ctx, 1)
}

func (s *baseSequencer) NextN(ctx context.Context, n int64) (int64, error) {
	if n <= 0 || n > s.size {
		return InitialSequenceValue, ErrInvalidSequence
	}

	for {
		if err := ctx.Err(); err != nil {
			return InitialSequenceValue, err
		}

		s.mu.Lock()
		nextSequence := s.nextSequence + n
		if s.hasAvailableCapacityLocked(nextSequence) {
			s.nextSequence = nextSequence
			s.mu.Unlock()

			return nextSequence, nil
		}

		request := s.capacityWaitRequestLocked(ctx, n, nextSequence)
		s.mu.Unlock()

		if s.waitStrategy == nil {
			runtime.Gosched()
			continue
		}
		if err := s.waitStrategy.WaitForCapacity(request); err != nil {
			return InitialSequenceValue, err
		}
	}
}

func (s *baseSequencer) TryNext() (int64, error) {
	return s.TryNextN(1)
}

func (s *baseSequencer) TryNextN(n int64) (int64, error) {
	if n <= 0 || n > s.size {
		return InitialSequenceValue, ErrInvalidSequence
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	nextSequence := s.nextSequence + n
	if !s.hasAvailableCapacityLocked(nextSequence) {
		return InitialSequenceValue, ErrInsufficientCapacity
	}

	s.nextSequence = nextSequence
	return nextSequence, nil
}

func (s *baseSequencer) Publish(sequence int64) {
	s.PublishRange(sequence, sequence)
}

func (s *baseSequencer) PublishRange(lo, hi int64) {
	if lo > hi {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for sequence := lo; sequence <= hi; sequence++ {
		s.available[sequence] = struct{}{}
	}
	s.advanceCursorLocked()
}

func (s *baseSequencer) Cursor() *Sequence {
	return s.cursor
}

func (s *baseSequencer) AddGatingSequences(sequences ...*Sequence) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, sequence := range sequences {
		if sequence == nil {
			continue
		}
		s.gatingSequences = append(s.gatingSequences, sequence)
	}
}

func (s *baseSequencer) RemoveGatingSequence(sequence *Sequence) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i, item := range s.gatingSequences {
		if item != sequence {
			continue
		}

		s.gatingSequences = append(
			s.gatingSequences[:i],
			s.gatingSequences[i+1:]...,
		)

		return true
	}

	return false
}

func (s *baseSequencer) HighestPublished(
	lowerBound int64,
	availableSequence int64,
) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.scanner.HighestPublished(availability.ScanRequest{
		LowerBound:        lowerBound,
		AvailableSequence: availableSequence,
		Availability:      mapAvailability{s.available},
	})
}

func (s *baseSequencer) Available(sequence int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.available[sequence]
	return ok
}

func (s *baseSequencer) hasAvailableCapacityLocked(nextSequence int64) bool {
	if len(s.gatingSequences) == 0 {
		return true
	}

	wrapPoint := nextSequence - s.size
	return wrapPoint <= s.minimumGatingSequenceLocked()
}

func (s *baseSequencer) minimumGatingSequenceLocked() int64 {
	minimum := s.gatingSequences[0].Value()
	for _, sequence := range s.gatingSequences[1:] {
		value := sequence.Value()
		if value < minimum {
			minimum = value
		}
	}

	return minimum
}

func (s *baseSequencer) capacityWaitRequestLocked(
	ctx context.Context,
	requestedSequences int64,
	nextSequence int64,
) CapacityWaitRequest {
	var gating SequenceReader
	if len(s.gatingSequences) > 0 {
		gating = s.gatingSequences[0]
	}

	return CapacityWaitRequest{
		Context:            ctx,
		RequestedSequences: requestedSequences,
		CurrentSequence:    s.nextSequence,
		WrapPoint:          nextSequence - s.size,
		GatingSequence:     gating,
	}
}

func (s *baseSequencer) advanceCursorLocked() {
	lowerBound := s.cursor.Value() + 1
	highestPublished := s.scanner.HighestPublished(availability.ScanRequest{
		LowerBound:        lowerBound,
		AvailableSequence: s.nextSequence,
		Availability:      mapAvailability{s.available},
	})
	if highestPublished < lowerBound {
		return
	}

	for sequence := lowerBound; sequence <= highestPublished; sequence++ {
		delete(s.available, sequence)
	}
	s.cursor.Store(highestPublished)
}

type mapAvailability struct {
	available map[int64]struct{}
}

func (a mapAvailability) Available(sequence int64) bool {
	_, ok := a.available[sequence]
	return ok
}
