package sequencer

type singleProducerSequencer struct {
	*baseSequencer
}

func NewSingleProducer(size int64, waitStrategy CapacityWaitStrategy) Sequencer {
	return &singleProducerSequencer{
		baseSequencer: newBaseSequencer(size, waitStrategy),
	}
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
