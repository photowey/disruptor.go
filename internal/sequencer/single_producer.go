package sequencer

type singleProducerSequencer struct {
	*baseSequencer
}

func NewSingleProducer(size int64, waitStrategy CapacityWaitStrategy) Sequencer {
	return &singleProducerSequencer{
		baseSequencer: newBaseSequencer(size, waitStrategy),
	}
}
