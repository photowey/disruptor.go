package disruptor

import "errors"

var (
	ErrAlerted              = errors.New("disruptor: alerted")
	ErrClosed               = errors.New("disruptor: closed")
	ErrInsufficientCapacity = errors.New("disruptor: insufficient capacity")
	ErrInvalidBufferSize    = errors.New("disruptor: invalid buffer size")
	ErrInvalidSequence      = errors.New("disruptor: invalid sequence")
)
