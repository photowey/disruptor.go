package disruptor_test

import (
	"context"
	"testing"

	"github.com/photowey/disruptor.go/pkg/disruptor"
)

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

func BenchmarkRingBufferNextNPublishRange(b *testing.B) {
	rb, err := disruptor.NewRingBuffer(
		disruptor.EventFactoryFunc[longEvent](func() longEvent { return longEvent{} }),
		65536,
	)
	if err != nil {
		b.Fatalf("new ring buffer: %v", err)
	}

	const batchSize int64 = 16
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
}
