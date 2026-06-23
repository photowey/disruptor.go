package sequencer

import (
	"context"
	"testing"
)

func TestMultiProducerUsesPerSlotAvailabilityBuffer(t *testing.T) {
	sequencer, ok := NewMultiProducer(8, nil).(*multiProducerSequencer)
	if !ok {
		t.Fatal("new multi producer should return multi producer sequencer")
	}

	if got := len(sequencer.availabilityBuffer); got != 8 {
		t.Fatalf("availability buffer length = %d, want 8", got)
	}
	if sequencer.indexMask != 7 {
		t.Fatalf("index mask = %d, want 7", sequencer.indexMask)
	}
	if sequencer.indexShift != 3 {
		t.Fatalf("index shift = %d, want 3", sequencer.indexShift)
	}
}

func TestMultiProducerDoesNotAdvanceCursorPastUnpublishedGap(t *testing.T) {
	ctx := context.Background()
	sequencer := NewMultiProducer(8, nil)

	lo, err := sequencer.Next(ctx)
	if err != nil {
		t.Fatalf("first next: %v", err)
	}
	hi, err := sequencer.Next(ctx)
	if err != nil {
		t.Fatalf("second next: %v", err)
	}

	sequencer.Publish(hi)
	if got := sequencer.Cursor().Value(); got != InitialSequenceValue {
		t.Fatalf("cursor = %d, want initial sequence before gap closes", got)
	}
	if !sequencer.Available(hi) {
		t.Fatalf("sequence %d should be marked available", hi)
	}

	sequencer.Publish(lo)
	if got := sequencer.Cursor().Value(); got != hi {
		t.Fatalf("cursor = %d, want %d after gap closes", got, hi)
	}
}

func TestSingleProducerPublishesRangeDirectly(t *testing.T) {
	ctx := context.Background()
	sequencer := NewSingleProducer(8, nil)

	hi, err := sequencer.NextN(ctx, 4)
	if err != nil {
		t.Fatalf("next batch: %v", err)
	}
	lo := hi - 3
	sequencer.PublishRange(lo, hi)

	if got := sequencer.Cursor().Value(); got != hi {
		t.Fatalf("cursor = %d, want %d", got, hi)
	}
}
