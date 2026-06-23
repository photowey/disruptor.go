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

package sequencer

import (
	"context"
	"errors"
	"testing"
)

type sequencerFactory func(int64, CapacityWaitStrategy) Sequencer

var errStopCapacityWait = errors.New("stop capacity wait")

type stopCapacityWaitStrategy struct{}

func (stopCapacityWaitStrategy) WaitForCapacity(request CapacityWaitRequest) error {
	if request.GatingSequence == nil {
		return ErrInsufficientCapacity
	}
	if request.GatingSequence.Value() != InitialSequenceValue {
		return ErrInsufficientCapacity
	}

	return errStopCapacityWait
}

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

func TestCapacityWaitRequestDoesNotAllocate(t *testing.T) {
	for name, newSequencer := range map[string]sequencerFactory{
		"single_producer": NewSingleProducer,
		"multi_producer":  NewMultiProducer,
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			sequencer := newSequencer(1, stopCapacityWaitStrategy{})
			sequencer.AddGatingSequences(NewSequence(InitialSequenceValue))

			if _, err := sequencer.Next(ctx); err != nil {
				t.Fatalf("first next: %v", err)
			}

			allocs := testing.AllocsPerRun(1000, func() {
				_, err := sequencer.Next(ctx)
				if !errors.Is(err, errStopCapacityWait) {
					t.Fatalf("next error = %v, want %v", err, errStopCapacityWait)
				}
			})
			if allocs != 0 {
				t.Fatalf("allocs = %.1f, want 0", allocs)
			}
		})
	}
}
