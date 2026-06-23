package sequencer

type minimumSequenceReader struct {
	sequences []*Sequence
}

func newMinimumSequenceReader(sequences []*Sequence) SequenceReader {
	nonNil := make([]*Sequence, 0, len(sequences))
	for _, sequence := range sequences {
		if sequence == nil {
			continue
		}
		nonNil = append(nonNil, sequence)
	}

	switch len(nonNil) {
	case 0:
		return nil
	case 1:
		return nonNil[0]
	default:
		return minimumSequenceReader{sequences: nonNil}
	}
}

func (r minimumSequenceReader) Value() int64 {
	if len(r.sequences) == 0 {
		return InitialSequenceValue
	}

	minimum := r.sequences[0].Value()
	for _, sequence := range r.sequences[1:] {
		value := sequence.Value()
		if value < minimum {
			minimum = value
		}
	}

	return minimum
}
