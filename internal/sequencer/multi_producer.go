package sequencer

import (
	"math/bits"
	"sync/atomic"

	"github.com/photowey/disruptor.go/internal/availability"
)

type multiProducerSequencer struct {
	*baseSequencer

	indexMask          int64
	indexShift         uint
	availabilityBuffer []int64
	scanner            availability.Scanner
}

func NewMultiProducer(size int64, waitStrategy CapacityWaitStrategy) Sequencer {
	availabilityBuffer := make([]int64, size)
	for i := range availabilityBuffer {
		availabilityBuffer[i] = InitialSequenceValue
	}

	return &multiProducerSequencer{
		baseSequencer:      newBaseSequencer(size, waitStrategy),
		indexMask:          size - 1,
		indexShift:         uint(bits.Len64(uint64(size - 1))),
		availabilityBuffer: availabilityBuffer,
		scanner:            availability.NewScalarScanner(),
	}
}

func (s *multiProducerSequencer) Publish(sequence int64) {
	s.PublishRange(sequence, sequence)
}

func (s *multiProducerSequencer) PublishRange(lo, hi int64) {
	if lo > hi {
		return
	}

	for sequence := lo; sequence <= hi; sequence++ {
		s.setAvailable(sequence)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.advanceCursorLocked()
}

func (s *multiProducerSequencer) HighestPublished(
	lowerBound int64,
	availableSequence int64,
) int64 {
	return s.scanner.HighestPublished(availability.ScanRequest{
		LowerBound:        lowerBound,
		AvailableSequence: availableSequence,
		Availability:      s,
	})
}

func (s *multiProducerSequencer) Available(sequence int64) bool {
	index := sequence & s.indexMask
	flag := s.availabilityFlag(sequence)

	return atomic.LoadInt64(&s.availabilityBuffer[index]) == flag
}

func (s *multiProducerSequencer) setAvailable(sequence int64) {
	index := sequence & s.indexMask
	flag := s.availabilityFlag(sequence)
	atomic.StoreInt64(&s.availabilityBuffer[index], flag)
}

func (s *multiProducerSequencer) availabilityFlag(sequence int64) int64 {
	return sequence >> s.indexShift
}

func (s *multiProducerSequencer) advanceCursorLocked() {
	lowerBound := s.cursor.Value() + 1
	highestPublished := s.HighestPublished(lowerBound, s.nextSequence)
	if highestPublished < lowerBound {
		return
	}

	s.cursor.Store(highestPublished)
}
