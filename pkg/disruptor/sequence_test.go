package disruptor_test

import (
	"testing"

	"github.com/photowey/disruptor.go/pkg/disruptor"
)

func TestSequenceStoresAndComparesValues(t *testing.T) {
	sequence := disruptor.NewSequence(disruptor.InitialSequenceValue)

	if got := sequence.Value(); got != disruptor.InitialSequenceValue {
		t.Fatalf("initial value = %d, want %d", got, disruptor.InitialSequenceValue)
	}

	sequence.Store(41)
	if got := sequence.Value(); got != 41 {
		t.Fatalf("stored value = %d, want 41", got)
	}

	if got := sequence.Add(1); got != 42 {
		t.Fatalf("add result = %d, want 42", got)
	}

	if swapped := sequence.CompareAndSwap(42, 100); !swapped {
		t.Fatal("compare-and-swap should succeed")
	}
	if got := sequence.Value(); got != 100 {
		t.Fatalf("compare-and-swap value = %d, want 100", got)
	}
}
