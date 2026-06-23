package disruptor

import sequencer "github.com/photowey/disruptor.go/internal/sequencer"

const InitialSequenceValue = sequencer.InitialSequenceValue

type Sequence = sequencer.Sequence

func NewSequence(initial int64) *Sequence {
	return sequencer.NewSequence(initial)
}
