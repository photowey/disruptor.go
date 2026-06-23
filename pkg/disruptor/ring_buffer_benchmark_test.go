package disruptor_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/photowey/disruptor.go/pkg/disruptor"
)

var benchmarkIntSink int64
var benchmarkEventSink *longEvent

func BenchmarkSequenceValue(b *testing.B) {
	sequence := disruptor.NewSequence(1024)

	b.ReportAllocs()
	for b.Loop() {
		benchmarkIntSink = sequence.Value()
	}
}

func BenchmarkSequenceStore(b *testing.B) {
	sequence := disruptor.NewSequence(disruptor.InitialSequenceValue)
	var value int64

	b.ReportAllocs()
	for b.Loop() {
		sequence.Store(value)
		value++
	}
}

func BenchmarkSequenceAdd(b *testing.B) {
	sequence := disruptor.NewSequence(disruptor.InitialSequenceValue)

	b.ReportAllocs()
	for b.Loop() {
		benchmarkIntSink = sequence.Add(1)
	}
}

func BenchmarkSequenceCompareAndSwap(b *testing.B) {
	sequence := disruptor.NewSequence(disruptor.InitialSequenceValue)
	value := disruptor.InitialSequenceValue

	b.ReportAllocs()
	for b.Loop() {
		if sequence.CompareAndSwap(value, value+1) {
			value++
		}
	}
}

func BenchmarkRingBufferGet(b *testing.B) {
	rb, err := disruptor.NewRingBuffer(
		disruptor.EventFactoryFunc[longEvent](func() longEvent { return longEvent{} }),
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
	rb, err := disruptor.NewRingBuffer(
		disruptor.EventFactoryFunc[longEvent](func() longEvent { return longEvent{} }),
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
	rb, err := disruptor.NewRingBuffer(
		disruptor.EventFactoryFunc[longEvent](func() longEvent { return longEvent{} }),
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
			rb, err := disruptor.NewRingBuffer(
				disruptor.EventFactoryFunc[longEvent](func() longEvent { return longEvent{} }),
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
	rb, err := disruptor.NewRingBuffer(
		disruptor.EventFactoryFunc[longEvent](func() longEvent { return longEvent{} }),
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
