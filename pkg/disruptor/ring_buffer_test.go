package disruptor_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/photowey/disruptor.go/pkg/disruptor"
)

type longEvent struct {
	Value int64
}

func TestNewRingBufferRejectsInvalidSizes(t *testing.T) {
	tests := []struct {
		name string
		size int
	}{
		{name: "zero", size: 0},
		{name: "negative", size: -1},
		{name: "not power of two", size: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := disruptor.NewRingBuffer(
				disruptor.EventFactoryFunc[longEvent](func() longEvent { return longEvent{} }),
				tt.size,
			)
			if !errors.Is(err, disruptor.ErrInvalidBufferSize) {
				t.Fatalf("error = %v, want ErrInvalidBufferSize", err)
			}
		})
	}
}

func TestRingBufferPreallocatesValueSlotsAndReturnsPointers(t *testing.T) {
	var nextValue int64
	rb, err := disruptor.NewRingBuffer(
		disruptor.EventFactoryFunc[longEvent](func() longEvent {
			nextValue++
			return longEvent{Value: nextValue}
		}),
		4,
	)
	if err != nil {
		t.Fatalf("new ring buffer: %v", err)
	}

	if got := rb.Get(0).Value; got != 1 {
		t.Fatalf("slot 0 value = %d, want 1", got)
	}
	if got := rb.Get(3).Value; got != 4 {
		t.Fatalf("slot 3 value = %d, want 4", got)
	}

	rb.Get(0).Value = 42
	if got := rb.Get(4).Value; got != 42 {
		t.Fatalf("wrapped slot value = %d, want 42", got)
	}
}

func TestNextNReturnsHighSequenceAndPublishRangeAdvancesBarrier(t *testing.T) {
	ctx := context.Background()
	rb := newTestRingBuffer(t, 8)

	hi, err := rb.NextN(ctx, 4)
	if err != nil {
		t.Fatalf("next batch: %v", err)
	}
	if hi != 3 {
		t.Fatalf("high sequence = %d, want 3", hi)
	}

	lo := hi - 4 + 1
	for sequence := lo; sequence <= hi; sequence++ {
		rb.Get(sequence).Value = sequence
	}
	rb.PublishRange(lo, hi)

	barrier := rb.NewBarrier()
	available, err := barrier.WaitFor(ctx, hi)
	if err != nil {
		t.Fatalf("wait for published range: %v", err)
	}
	if available != hi {
		t.Fatalf("available sequence = %d, want %d", available, hi)
	}
}

func TestRingBufferSingleProducerOptionPublishesEvents(t *testing.T) {
	ctx := context.Background()
	rb, err := disruptor.NewRingBuffer(
		disruptor.EventFactoryFunc[longEvent](func() longEvent { return longEvent{} }),
		8,
		disruptor.WithProducerType(disruptor.ProducerTypeSingle),
	)
	if err != nil {
		t.Fatalf("new ring buffer: %v", err)
	}

	sequence, err := rb.Next(ctx)
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	rb.Get(sequence).Value = 42
	rb.Publish(sequence)

	barrier := rb.NewBarrier()
	available, err := barrier.WaitFor(ctx, sequence)
	if err != nil {
		t.Fatalf("wait for published sequence: %v", err)
	}
	if available != sequence {
		t.Fatalf("available sequence = %d, want %d", available, sequence)
	}
	if got := rb.Get(sequence).Value; got != 42 {
		t.Fatalf("event value = %d, want 42", got)
	}
}

func TestPublishEventPublishesClaimedSequenceWhenTranslatorPanics(t *testing.T) {
	ctx := context.Background()
	rb := newTestRingBuffer(t, 4)

	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("expected translator panic")
		}

		barrier := rb.NewBarrier()
		available, err := barrier.WaitFor(ctx, 0)
		if err != nil {
			t.Fatalf("wait for panic-published sequence: %v", err)
		}
		if available != 0 {
			t.Fatalf("available sequence = %d, want 0", available)
		}
	}()

	_ = rb.PublishEvent(ctx, disruptor.EventTranslatorFunc[longEvent](func(request disruptor.TranslateRequest[longEvent]) {
		request.Event.Value = 7
		panic("translator failed")
	}))
}

func TestTryNextReturnsInsufficientCapacityWhenGatingSequenceBlocks(t *testing.T) {
	rb := newTestRingBuffer(t, 2)
	gating := disruptor.NewSequence(disruptor.InitialSequenceValue)
	rb.AddGatingSequences(gating)

	if _, err := rb.TryNext(); err != nil {
		t.Fatalf("first try next: %v", err)
	}
	if _, err := rb.TryNext(); err != nil {
		t.Fatalf("second try next: %v", err)
	}
	if _, err := rb.TryNext(); !errors.Is(err, disruptor.ErrInsufficientCapacity) {
		t.Fatalf("third try next error = %v, want ErrInsufficientCapacity", err)
	}

	gating.Store(0)
	if _, err := rb.TryNext(); err != nil {
		t.Fatalf("try next after gating advances: %v", err)
	}
}

func TestNextReturnsContextErrorWhenCapacityWaitIsCancelled(t *testing.T) {
	rb := newTestRingBuffer(t, 1)
	gating := disruptor.NewSequence(disruptor.InitialSequenceValue)
	rb.AddGatingSequences(gating)

	if _, err := rb.Next(context.Background()); err != nil {
		t.Fatalf("first next: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := rb.Next(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled next error = %v, want context.Canceled", err)
	}
}

func TestRemoveGatingSequence(t *testing.T) {
	rb := newTestRingBuffer(t, 1)
	gating := disruptor.NewSequence(disruptor.InitialSequenceValue)
	rb.AddGatingSequences(gating)

	if removed := rb.RemoveGatingSequence(gating); !removed {
		t.Fatal("remove gating sequence should return true")
	}
	if removed := rb.RemoveGatingSequence(gating); removed {
		t.Fatal("removing missing gating sequence should return false")
	}

	if _, err := rb.Next(context.Background()); err != nil {
		t.Fatalf("next without gating sequence: %v", err)
	}
	if _, err := rb.Next(context.Background()); err != nil {
		t.Fatalf("second next without gating sequence: %v", err)
	}
}

func TestNextUnblocksWhenGatingSequenceIsRemoved(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rb := newTestRingBuffer(t, 1)
	gating := disruptor.NewSequence(disruptor.InitialSequenceValue)
	rb.AddGatingSequences(gating)

	if _, err := rb.Next(ctx); err != nil {
		t.Fatalf("first next: %v", err)
	}

	result := make(chan error, 1)
	go func() {
		_, err := rb.Next(ctx)
		result <- err
	}()

	select {
	case err := <-result:
		t.Fatalf("next returned before gating sequence was removed: %v", err)
	case <-time.After(10 * time.Millisecond):
	}

	if removed := rb.RemoveGatingSequence(gating); !removed {
		t.Fatal("remove gating sequence should return true")
	}

	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("next after gating sequence removal: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for next after gating sequence removal")
	}
}

func newTestRingBuffer(t *testing.T, size int) *disruptor.RingBuffer[longEvent] {
	t.Helper()

	return newTestRingBufferWithOptions(t, size)
}

func newTestRingBufferWithOptions(
	t *testing.T,
	size int,
	opts ...disruptor.RingBufferOption,
) *disruptor.RingBuffer[longEvent] {
	t.Helper()

	rb, err := disruptor.NewRingBuffer(
		disruptor.EventFactoryFunc[longEvent](func() longEvent { return longEvent{} }),
		size,
		opts...,
	)
	if err != nil {
		t.Fatalf("new ring buffer: %v", err)
	}

	return rb
}
