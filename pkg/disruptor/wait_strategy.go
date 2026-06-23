package disruptor

import (
	"context"
	"runtime"
	"sync"

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
	once       sync.Once
	mu         sync.Mutex
	cond       *sync.Cond
	generation uint64
}

func NewBlockingWaitStrategy() WaitStrategy {
	strategy := &BlockingWaitStrategy{}
	strategy.init()

	return strategy
}

func (s *BlockingWaitStrategy) WaitFor(request WaitRequest) (int64, error) {
	s.init()
	if request.Context.Done() != nil {
		stopContextSignal := context.AfterFunc(request.Context, s.SignalAll)
		defer stopContextSignal()
	}

	for {
		generation := s.generationValue()
		if err := request.Context.Err(); err != nil {
			return InitialSequenceValue, err
		}
		if request.Barrier != nil {
			if err := request.Barrier.CheckAlert(); err != nil {
				return InitialSequenceValue, err
			}
		}

		available := readAvailableSequence(request)
		if available >= request.RequestedSequence {
			return available, nil
		}

		s.waitForSignal(generation, request.Context)
	}
}

func (s *BlockingWaitStrategy) WaitForCapacity(request CapacityWaitRequest) error {
	s.init()
	if request.Context.Done() != nil {
		stopContextSignal := context.AfterFunc(request.Context, s.SignalAll)
		defer stopContextSignal()
	}

	if err := request.Context.Err(); err != nil {
		return err
	}

	generation := s.generationValue()
	if capacityAvailable(request) {
		return nil
	}

	s.waitForSignal(generation, request.Context)
	return request.Context.Err()
}

func (s *BlockingWaitStrategy) SignalAll() {
	s.init()

	s.mu.Lock()
	defer s.mu.Unlock()

	s.generation++
	s.cond.Broadcast()
}

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

func (s *BlockingWaitStrategy) init() {
	s.once.Do(func() {
		s.cond = sync.NewCond(&s.mu)
	})
}

func (s *BlockingWaitStrategy) generationValue() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.generation
}

func (s *BlockingWaitStrategy) waitForSignal(
	generation uint64,
	ctx context.Context,
) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for generation == s.generation && ctx.Err() == nil {
		s.cond.Wait()
	}
}

func capacityAvailable(request CapacityWaitRequest) bool {
	if request.GatingSequence == nil {
		return true
	}

	return request.WrapPoint <= request.GatingSequence.Value()
}
