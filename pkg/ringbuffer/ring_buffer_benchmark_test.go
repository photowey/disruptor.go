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

package ringbuffer_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/photowey/disruptor.go/pkg/event"
	"github.com/photowey/disruptor.go/pkg/ringbuffer"
	"github.com/photowey/disruptor.go/pkg/sequence"
)

var benchmarkIntSink int64
var benchmarkEventSink *longEvent

func BenchmarkSequenceValue(b *testing.B) {
	seq := sequence.New(1024)

	b.ReportAllocs()
	for b.Loop() {
		benchmarkIntSink = seq.Value()
	}
}

func BenchmarkSequenceStore(b *testing.B) {
	seq := sequence.New(sequence.InitialValue)
	var value int64

	b.ReportAllocs()
	for b.Loop() {
		seq.Store(value)
		value++
	}
}

func BenchmarkSequenceAdd(b *testing.B) {
	seq := sequence.New(sequence.InitialValue)

	b.ReportAllocs()
	for b.Loop() {
		benchmarkIntSink = seq.Add(1)
	}
}

func BenchmarkSequenceCompareAndSwap(b *testing.B) {
	seq := sequence.New(sequence.InitialValue)
	value := sequence.InitialValue

	b.ReportAllocs()
	for b.Loop() {
		if seq.CompareAndSwap(value, value+1) {
			value++
		}
	}
}

func BenchmarkRingBufferGet(b *testing.B) {
	rb, err := ringbuffer.New(
		event.FactoryFunc[longEvent](func() longEvent { return longEvent{} }),
		65536,
	)
	if err != nil {
		b.Fatalf("new ring buffer: %v", err)
	}

	var sequence int64
	b.ReportAllocs()
	for b.Loop() {
		benchmarkEventSink = rb.Get(sequence)
		sequence++
	}
}

func BenchmarkRingBufferNextPublish(b *testing.B) {
	rb, err := ringbuffer.New(
		event.FactoryFunc[longEvent](func() longEvent { return longEvent{} }),
		65536,
	)
	if err != nil {
		b.Fatalf("new ring buffer: %v", err)
	}

	ctx := context.Background()
	b.ReportAllocs()

	for b.Loop() {
		sequence, err := rb.Next(ctx)
		if err != nil {
			b.Fatalf("next: %v", err)
		}
		rb.Get(sequence).Value = sequence
		rb.Publish(sequence)
	}
}

func BenchmarkRingBufferTryNextPublish(b *testing.B) {
	rb, err := ringbuffer.New(
		event.FactoryFunc[longEvent](func() longEvent { return longEvent{} }),
		65536,
	)
	if err != nil {
		b.Fatalf("new ring buffer: %v", err)
	}

	b.ReportAllocs()
	for b.Loop() {
		sequence, err := rb.TryNext()
		if err != nil {
			b.Fatalf("try next: %v", err)
		}
		rb.Get(sequence).Value = sequence
		rb.Publish(sequence)
	}
}

func BenchmarkRingBufferNextNPublishRange(b *testing.B) {
	for _, batchSize := range []int64{1, 4, 16, 64, 256} {
		b.Run(fmt.Sprintf("batch_%d", batchSize), func(b *testing.B) {
			rb, err := ringbuffer.New(
				event.FactoryFunc[longEvent](func() longEvent { return longEvent{} }),
				65536,
			)
			if err != nil {
				b.Fatalf("new ring buffer: %v", err)
			}

			ctx := context.Background()
			b.ReportAllocs()

			for b.Loop() {
				hi, err := rb.NextN(ctx, batchSize)
				if err != nil {
					b.Fatalf("next batch: %v", err)
				}
				lo := hi - batchSize + 1
				for sequence := lo; sequence <= hi; sequence++ {
					rb.Get(sequence).Value = sequence
				}
				rb.PublishRange(lo, hi)
			}
		})
	}
}

func BenchmarkBarrierWaitForPublishedSequence(b *testing.B) {
	rb, err := ringbuffer.New(
		event.FactoryFunc[longEvent](func() longEvent { return longEvent{} }),
		65536,
	)
	if err != nil {
		b.Fatalf("new ring buffer: %v", err)
	}

	sequence, err := rb.Next(context.Background())
	if err != nil {
		b.Fatalf("next: %v", err)
	}
	rb.Publish(sequence)

	barrier := rb.NewBarrier()
	ctx := context.Background()
	b.ReportAllocs()

	for b.Loop() {
		available, err := barrier.WaitFor(ctx, sequence)
		if err != nil {
			b.Fatalf("wait for sequence: %v", err)
		}
		benchmarkIntSink = available
	}
}
