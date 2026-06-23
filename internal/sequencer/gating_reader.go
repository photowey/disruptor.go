package sequencer

const availableCapacityValue = int64(1<<63 - 1)

type baseSequencerGatingReader struct {
	sequencer *baseSequencer
}

func (r baseSequencerGatingReader) Value() int64 {
	r.sequencer.mu.Lock()
	defer r.sequencer.mu.Unlock()

	if len(r.sequencer.gatingSequences) == 0 {
		return availableCapacityValue
	}

	return r.sequencer.minimumGatingSequenceLocked()
}

type singleProducerGatingReader struct {
	sequencer *singleProducerSequencer
}

func (r singleProducerGatingReader) Value() int64 {
	r.sequencer.gatingMu.RLock()
	defer r.sequencer.gatingMu.RUnlock()

	if len(r.sequencer.gatingSequences) == 0 {
		return availableCapacityValue
	}

	minimum := r.sequencer.gatingSequences[0].Value()
	for _, sequence := range r.sequencer.gatingSequences[1:] {
		value := sequence.Value()
		if value < minimum {
			minimum = value
		}
	}

	return minimum
}
