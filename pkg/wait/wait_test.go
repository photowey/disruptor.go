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

package wait_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/photowey/disruptor.go/pkg/event"
	"github.com/photowey/disruptor.go/pkg/ringbuffer"
	"github.com/photowey/disruptor.go/pkg/sequence"
	"github.com/photowey/disruptor.go/pkg/wait"
)

func TestBarrierWaitRequestIncludesDependentSequence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	waitStrategy := &capturingWaitStrategy{
		waitFor: func(request wait.Request) (int64, error) {
			cancel()
			return sequence.InitialValue, nil
		},
	}
	rb := newTestRingBufferWithOptions(
		t,
		8,
		ringbuffer.WithWaitStrategy(waitStrategy),
	)
	dependency := sequence.New(sequence.InitialValue)
	barrier := rb.NewBarrier(dependency)

	sequenceValue, err := rb.Next(context.Background())
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	rb.Publish(sequenceValue)

	_, err = barrier.WaitFor(ctx, sequenceValue)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("wait error = %v, want context.Canceled", err)
	}

	request := waitStrategy.waitRequest()
	if request.DependentSequence == nil {
		t.Fatal("dependent sequence should be passed to wait strategy")
	}
	if got := request.DependentSequence.Value(); got != sequence.InitialValue {
		t.Fatalf("dependent sequence value = %d, want initial sequence", got)
	}
}

func TestCapacityWaitRequestUsesSlowestGatingSequence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	waitStrategy := &capturingWaitStrategy{
		waitForCapacity: func(request wait.CapacityRequest) error {
			cancel()
			return context.Canceled
		},
	}
	rb := newTestRingBufferWithOptions(
		t,
		1,
		ringbuffer.WithWaitStrategy(waitStrategy),
	)
	fastConsumer := sequence.New(0)
	slowestConsumer := sequence.New(sequence.InitialValue)
	rb.AddGatingSequences(fastConsumer, slowestConsumer)

	if _, err := rb.Next(context.Background()); err != nil {
		t.Fatalf("first next: %v", err)
	}
	_, err := rb.Next(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("second next error = %v, want context.Canceled", err)
	}

	request := waitStrategy.capacityRequest()
	if request.GatingSequence == nil {
		t.Fatal("gating sequence should be passed to wait strategy")
	}
	if got := request.GatingSequence.Value(); got != sequence.InitialValue {
		t.Fatalf("gating sequence value = %d, want slowest consumer", got)
	}
}

func TestBlockingWaitStrategyWaitsUntilSignal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	strategy := wait.NewBlockingStrategy()
	cursor := sequence.New(sequence.InitialValue)
	done := make(chan error, 1)
	task := blockingWaitStrategyTask{
		ctx:      ctx,
		strategy: strategy,
		cursor:   cursor,
		done:     done,
	}
	go task.run()

	select {
	case err := <-done:
		t.Fatalf("wait returned before signal: %v", err)
	case <-time.After(10 * time.Millisecond):
	}

	cursor.Store(0)
	strategy.SignalAll()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("wait error after signal: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for signal")
	}
}

type blockingWaitStrategyTask struct {
	ctx      context.Context
	strategy wait.Strategy
	cursor   *sequence.Sequence
	done     chan<- error
}

func (task blockingWaitStrategyTask) run() {
	_, err := task.strategy.WaitFor(wait.Request{
		Context:           task.ctx,
		RequestedSequence: 0,
		CursorSequence:    task.cursor,
	})
	task.done <- err
}

func TestBlockingWaitStrategyCapacitySignalCannotBeLost(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	strategy := wait.NewBlockingStrategy()
	reader := &signalingSequenceReader{
		value: sequence.InitialValue,
		signal: func() {
			strategy.SignalAll()
		},
	}

	err := strategy.WaitForCapacity(wait.CapacityRequest{
		Context:        ctx,
		WrapPoint:      0,
		GatingSequence: reader,
	})
	if err != nil {
		t.Fatalf("wait for capacity: %v", err)
	}
}

type capturingWaitStrategy struct {
	mu sync.Mutex

	capturedWaitRequest     wait.Request
	capturedCapacityRequest wait.CapacityRequest
	waitFor                 func(wait.Request) (int64, error)
	waitForCapacity         func(wait.CapacityRequest) error
}

func (s *capturingWaitStrategy) WaitFor(
	request wait.Request,
) (int64, error) {
	s.mu.Lock()
	s.capturedWaitRequest = request
	s.mu.Unlock()

	if s.waitFor != nil {
		return s.waitFor(request)
	}

	return sequence.InitialValue, nil
}

func (s *capturingWaitStrategy) WaitForCapacity(
	request wait.CapacityRequest,
) error {
	s.mu.Lock()
	s.capturedCapacityRequest = request
	s.mu.Unlock()

	if s.waitForCapacity != nil {
		return s.waitForCapacity(request)
	}

	return nil
}

func (s *capturingWaitStrategy) SignalAll() {}

func (s *capturingWaitStrategy) waitRequest() wait.Request {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.capturedWaitRequest
}

func (s *capturingWaitStrategy) capacityRequest() wait.CapacityRequest {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.capturedCapacityRequest
}

type signalingSequenceReader struct {
	once sync.Once

	value  int64
	signal func()
}

type waitEvent struct{}

func newTestRingBufferWithOptions(
	t *testing.T,
	size int,
	opts ...ringbuffer.Option,
) *ringbuffer.RingBuffer[waitEvent] {
	t.Helper()

	rb, err := ringbuffer.New(
		event.FactoryFunc[waitEvent](func() waitEvent { return waitEvent{} }),
		size,
		opts...,
	)
	if err != nil {
		t.Fatalf("new ring buffer: %v", err)
	}

	return rb
}

func (r *signalingSequenceReader) Value() int64 {
	r.once.Do(r.signal)

	return r.value
}
