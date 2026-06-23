package sequencer

type multiProducerSequencer struct {
	*baseSequencer
}

func NewMultiProducer(size int64, waitStrategy CapacityWaitStrategy) Sequencer {
	return &multiProducerSequencer{
		baseSequencer: newBaseSequencer(size, waitStrategy),
	}
}
