// Copyright © 2026-present The Disruptor.go Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disruptor

import (
	"context"
	"runtime"
	"sync"

	sequencer "github.com/photowey/disruptor.go/internal/sequencer"
)

// SequenceReader exposes a readable sequence value.
type SequenceReader = sequencer.SequenceReader

// WaitStrategy waits for sequence availability and producer capacity.
type WaitStrategy interface {
	WaitFor(request WaitRequest) (int64, error)
	WaitForCapacity(request CapacityWaitRequest) error
	SignalAll()
}

// WaitRequest carries all state needed to wait for a consumer sequence.
type WaitRequest struct {
	Context           context.Context
	RequestedSequence int64
	CursorSequence    SequenceReader
	DependentSequence SequenceReader
	Barrier           Barrier
}

// CapacityWaitRequest carries all state needed to wait for producer capacity.
type CapacityWaitRequest = sequencer.CapacityWaitRequest

// BlockingWaitStrategy uses a condition variable to block waiters.
type BlockingWaitStrategy struct {
	once       sync.Once
	mu         sync.Mutex
	cond       *sync.Cond
	generation uint64
}

// NewBlockingWaitStrategy constructs a blocking wait strategy.
func NewBlockingWaitStrategy() WaitStrategy {
	strategy := &BlockingWaitStrategy{}
	strategy.init()

	return strategy
}

// WaitFor waits until the requested sequence is available.
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

// WaitForCapacity waits until a producer can safely claim capacity.
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

// SignalAll wakes blocked waiters.
func (s *BlockingWaitStrategy) SignalAll() {
	s.init()

	s.mu.Lock()
	defer s.mu.Unlock()

	s.generation++
	s.cond.Broadcast()
}

// BusySpinWaitStrategy polls for progress and yields the processor.
type BusySpinWaitStrategy struct{}

// NewBusySpinWaitStrategy constructs a busy-spin wait strategy.
func NewBusySpinWaitStrategy() WaitStrategy {
	return BusySpinWaitStrategy{}
}

// WaitFor polls once for sequence availability.
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

// WaitForCapacity polls once for producer capacity.
func (s BusySpinWaitStrategy) WaitForCapacity(request CapacityWaitRequest) error {
	if err := request.Context.Err(); err != nil {
		return err
	}

	runtime.Gosched()
	return nil
}

// SignalAll is a no-op for busy-spin waiting.
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
