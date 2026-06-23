package disruptor

import (
	"errors"

	sequencer "github.com/photowey/disruptor.go/internal/sequencer"
)

var (
	ErrAlerted              = errors.New("disruptor: alerted")
	ErrClosed               = errors.New("disruptor: closed")
	ErrInsufficientCapacity = sequencer.ErrInsufficientCapacity
	ErrInvalidBufferSize    = errors.New("disruptor: invalid buffer size")
	ErrInvalidSequence      = sequencer.ErrInvalidSequence
)
