package sequencer

import "errors"

var (
	ErrInsufficientCapacity = errors.New("disruptor: insufficient capacity")
	ErrInvalidSequence      = errors.New("disruptor: invalid sequence")
)
