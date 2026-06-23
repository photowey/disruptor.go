package sequencer

import (
	"context"
	"fmt"
	"runtime"
	"testing"
)

type benchmarkWaitStrategy struct{}

func (benchmarkWaitStrategy) WaitForCapacity(request CapacityWaitRequest) error {
	if err := request.Context.Err(); err != nil {
		return err
	}

	runtime.Gosched()
	return nil
}

func BenchmarkSingleProducerClaimPublish(b *testing.B) {
	benchmarkSequencerClaimPublish(
		b,
		NewSingleProducer(65536, benchmarkWaitStrategy{}),
	)
}

func BenchmarkMultiProducerClaimPublish(b *testing.B) {
	benchmarkSequencerClaimPublish(
		b,
		NewMultiProducer(65536, benchmarkWaitStrategy{}),
	)
}

func BenchmarkSequencerBatchClaimPublish(b *testing.B) {
	for _, batchSize := range []int64{1, 4, 16, 64, 256} {
		b.Run(fmt.Sprintf("batch_%d", batchSize), func(b *testing.B) {
			benchmarkSequencerBatchClaimPublish(
				b,
				NewMultiProducer(65536, benchmarkWaitStrategy{}),
				batchSize,
			)
		})
	}
}

func benchmarkSequencerClaimPublish(b *testing.B, sequencer Sequencer) {
	b.Helper()

	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		sequence, err := sequencer.Next(ctx)
		if err != nil {
			b.Fatalf("next: %v", err)
		}
		sequencer.Publish(sequence)
	}
}

func benchmarkSequencerBatchClaimPublish(
	b *testing.B,
	sequencer Sequencer,
	batchSize int64,
) {
	b.Helper()

	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		hi, err := sequencer.NextN(ctx, batchSize)
		if err != nil {
			b.Fatalf("next batch: %v", err)
		}
		lo := hi - batchSize + 1
		sequencer.PublishRange(lo, hi)
	}
}
