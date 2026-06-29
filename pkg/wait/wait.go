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

package wait

import (
	"context"
	"runtime"
	"sync"

	sequencer "github.com/photowey/disruptor.go/internal/sequencer"
	"github.com/photowey/disruptor.go/pkg/sequence"
)

// Barrier exposes alert state used while waiting for sequence availability.
type Barrier interface {
	CheckAlert() error
}

// Strategy waits for sequence availability and producer capacity.
type Strategy interface {
	WaitFor(request Request) (int64, error)
	WaitForCapacity(request CapacityRequest) error
	SignalAll()
}

// Request carries all state needed to wait for a consumer sequence.
type Request struct {
	Context           context.Context
	RequestedSequence int64
	CursorSequence    sequence.Reader
	DependentSequence sequence.Reader
	Barrier           Barrier
}

// CapacityRequest carries all state needed to wait for producer capacity.
type CapacityRequest = sequencer.CapacityWaitRequest

// BlockingStrategy uses a condition variable to block waiters.
type BlockingStrategy struct {
	once       sync.Once
	mu         sync.Mutex
	cond       *sync.Cond
	generation uint64
}

// NewBlockingStrategy constructs a blocking wait strategy.
func NewBlockingStrategy() Strategy {
	strategy := &BlockingStrategy{}
	strategy.init()

	return strategy
}

// WaitFor waits until the requested sequence is available.
func (s *BlockingStrategy) WaitFor(request Request) (int64, error) {
	s.init()
	if request.Context.Done() != nil {
		stopContextSignal := context.AfterFunc(request.Context, s.SignalAll)
		defer stopContextSignal()
	}

	for {
		generation := s.generationValue()
		if err := request.Context.Err(); err != nil {
			return sequence.InitialValue, err
		}
		if request.Barrier != nil {
			if err := request.Barrier.CheckAlert(); err != nil {
				return sequence.InitialValue, err
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
func (s *BlockingStrategy) WaitForCapacity(request CapacityRequest) error {
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
func (s *BlockingStrategy) SignalAll() {
	s.init()

	s.mu.Lock()
	defer s.mu.Unlock()

	s.generation++
	s.cond.Broadcast()
}

// BusySpinStrategy polls for progress and yields the processor.
type BusySpinStrategy struct{}

// NewBusySpinStrategy constructs a busy-spin wait strategy.
func NewBusySpinStrategy() Strategy {
	return BusySpinStrategy{}
}

// WaitFor polls once for sequence availability.
func (s BusySpinStrategy) WaitFor(request Request) (int64, error) {
	if err := request.Context.Err(); err != nil {
		return sequence.InitialValue, err
	}
	if request.Barrier != nil {
		if err := request.Barrier.CheckAlert(); err != nil {
			return sequence.InitialValue, err
		}
	}

	runtime.Gosched()
	return readAvailableSequence(request), nil
}

// WaitForCapacity polls once for producer capacity.
func (s BusySpinStrategy) WaitForCapacity(request CapacityRequest) error {
	if err := request.Context.Err(); err != nil {
		return err
	}

	runtime.Gosched()
	return nil
}

// SignalAll is a no-op for busy-spin waiting.
func (s BusySpinStrategy) SignalAll() {}

func readAvailableSequence(request Request) int64 {
	if request.CursorSequence == nil {
		return sequence.InitialValue
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

func (s *BlockingStrategy) init() {
	s.once.Do(s.initializeCond)
}

func (s *BlockingStrategy) initializeCond() {
	s.cond = sync.NewCond(&s.mu)
}

func (s *BlockingStrategy) generationValue() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.generation
}

func (s *BlockingStrategy) waitForSignal(
	generation uint64,
	ctx context.Context,
) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for generation == s.generation && ctx.Err() == nil {
		s.cond.Wait()
	}
}

func capacityAvailable(request CapacityRequest) bool {
	if request.GatingSequence == nil {
		return true
	}

	return request.WrapPoint <= request.GatingSequence.Value()
}
