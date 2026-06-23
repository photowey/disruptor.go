package sequencer

import "context"

type SequenceReader interface {
	Value() int64
}

type CapacityWaitStrategy interface {
	WaitForCapacity(request CapacityWaitRequest) error
}

type CapacityWaitRequest struct {
	Context            context.Context
	RequestedSequences int64
	CurrentSequence    int64
	WrapPoint          int64
	GatingSequence     SequenceReader
}
