package disruptor_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/photowey/disruptor.go/pkg/disruptor"
)

func TestBarrierWaitRequestIncludesDependentSequence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	waitStrategy := &capturingWaitStrategy{
		waitFor: func(request disruptor.WaitRequest) (int64, error) {
			cancel()
			return disruptor.InitialSequenceValue, nil
		},
	}
	rb := newTestRingBufferWithOptions(
		t,
		8,
		disruptor.WithWaitStrategy(waitStrategy),
	)
	dependency := disruptor.NewSequence(disruptor.InitialSequenceValue)
	barrier := rb.NewBarrier(dependency)

	sequence, err := rb.Next(context.Background())
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	rb.Publish(sequence)

	_, err = barrier.WaitFor(ctx, sequence)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("wait error = %v, want context.Canceled", err)
	}

	request := waitStrategy.waitRequest()
	if request.DependentSequence == nil {
		t.Fatal("dependent sequence should be passed to wait strategy")
	}
	if got := request.DependentSequence.Value(); got != disruptor.InitialSequenceValue {
		t.Fatalf("dependent sequence value = %d, want initial sequence", got)
	}
}

func TestCapacityWaitRequestUsesSlowestGatingSequence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	waitStrategy := &capturingWaitStrategy{
		waitForCapacity: func(request disruptor.CapacityWaitRequest) error {
			cancel()
			return context.Canceled
		},
	}
	rb := newTestRingBufferWithOptions(
		t,
		1,
		disruptor.WithWaitStrategy(waitStrategy),
	)
	fastConsumer := disruptor.NewSequence(0)
	slowestConsumer := disruptor.NewSequence(disruptor.InitialSequenceValue)
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
	if got := request.GatingSequence.Value(); got != disruptor.InitialSequenceValue {
		t.Fatalf("gating sequence value = %d, want slowest consumer", got)
	}
}

func TestBlockingWaitStrategyWaitsUntilSignal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	strategy := disruptor.NewBlockingWaitStrategy()
	cursor := disruptor.NewSequence(disruptor.InitialSequenceValue)
	done := make(chan error, 1)
	go func() {
		_, err := strategy.WaitFor(disruptor.WaitRequest{
			Context:           ctx,
			RequestedSequence: 0,
			CursorSequence:    cursor,
		})
		done <- err
	}()

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

func TestBlockingWaitStrategyCapacitySignalCannotBeLost(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	strategy := disruptor.NewBlockingWaitStrategy()
	reader := &signalingSequenceReader{
		value: disruptor.InitialSequenceValue,
		signal: func() {
			strategy.SignalAll()
		},
	}

	err := strategy.WaitForCapacity(disruptor.CapacityWaitRequest{
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

	capturedWaitRequest     disruptor.WaitRequest
	capturedCapacityRequest disruptor.CapacityWaitRequest
	waitFor                 func(disruptor.WaitRequest) (int64, error)
	waitForCapacity         func(disruptor.CapacityWaitRequest) error
}

func (s *capturingWaitStrategy) WaitFor(
	request disruptor.WaitRequest,
) (int64, error) {
	s.mu.Lock()
	s.capturedWaitRequest = request
	s.mu.Unlock()

	if s.waitFor != nil {
		return s.waitFor(request)
	}

	return disruptor.InitialSequenceValue, nil
}

func (s *capturingWaitStrategy) WaitForCapacity(
	request disruptor.CapacityWaitRequest,
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

func (s *capturingWaitStrategy) waitRequest() disruptor.WaitRequest {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.capturedWaitRequest
}

func (s *capturingWaitStrategy) capacityRequest() disruptor.CapacityWaitRequest {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.capturedCapacityRequest
}

type signalingSequenceReader struct {
	once sync.Once

	value  int64
	signal func()
}

func (r *signalingSequenceReader) Value() int64 {
	r.once.Do(r.signal)

	return r.value
}
