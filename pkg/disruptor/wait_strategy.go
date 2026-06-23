package disruptor

import (
	"context"
	"runtime"
	"time"

	sequencer "github.com/photowey/disruptor.go/internal/sequencer"
)

type SequenceReader = sequencer.SequenceReader

type WaitStrategy interface {
	WaitFor(request WaitRequest) (int64, error)
	WaitForCapacity(request CapacityWaitRequest) error
	SignalAll()
}

type WaitRequest struct {
	Context           context.Context
	RequestedSequence int64
	CursorSequence    SequenceReader
	DependentSequence SequenceReader
	Barrier           Barrier
}

type CapacityWaitRequest = sequencer.CapacityWaitRequest

type BlockingWaitStrategy struct {
	interval time.Duration
}

func NewBlockingWaitStrategy() WaitStrategy {
	return &BlockingWaitStrategy{
		interval: time.Microsecond,
	}
}

func (s *BlockingWaitStrategy) WaitFor(request WaitRequest) (int64, error) {
	if err := request.Context.Err(); err != nil {
		return InitialSequenceValue, err
	}
	if request.Barrier != nil {
		if err := request.Barrier.CheckAlert(); err != nil {
			return InitialSequenceValue, err
		}
	}

	timer := time.NewTimer(s.interval)
	defer timer.Stop()

	select {
	case <-request.Context.Done():
		return InitialSequenceValue, request.Context.Err()
	case <-timer.C:
		return readAvailableSequence(request), nil
	}
}

func (s *BlockingWaitStrategy) WaitForCapacity(request CapacityWaitRequest) error {
	if err := request.Context.Err(); err != nil {
		return err
	}

	timer := time.NewTimer(s.interval)
	defer timer.Stop()

	select {
	case <-request.Context.Done():
		return request.Context.Err()
	case <-timer.C:
		return nil
	}
}

func (s *BlockingWaitStrategy) SignalAll() {}

type BusySpinWaitStrategy struct{}

func NewBusySpinWaitStrategy() WaitStrategy {
	return BusySpinWaitStrategy{}
}

func (s BusySpinWaitStrategy) WaitFor(request WaitRequest) (int64, error) {
	if err := request.Context.Err(); err != nil {
		return InitialSequenceValue, err
	}
	if request.Barrier != nil {
		if err := request.Barrier.CheckAlert(); err != nil {
			return InitialSequenceValue, err
		}
	}

	runtime.Gosched()
	return readAvailableSequence(request), nil
}

func (s BusySpinWaitStrategy) WaitForCapacity(request CapacityWaitRequest) error {
	if err := request.Context.Err(); err != nil {
		return err
	}

	runtime.Gosched()
	return nil
}

func (s BusySpinWaitStrategy) SignalAll() {}

func readAvailableSequence(request WaitRequest) int64 {
	if request.CursorSequence == nil {
		return InitialSequenceValue
	}

	available := request.CursorSequence.Value()
	if request.DependentSequence == nil {
		return available
	}

	dependent := request.DependentSequence.Value()
	if dependent < available {
		return dependent
	}

	return available
}
