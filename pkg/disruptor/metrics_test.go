package disruptor_test

import (
	"context"
	"testing"

	"github.com/photowey/disruptor.go/pkg/disruptor"
)

func TestRingBufferReportsPublishMetricWhenSinkConfigured(t *testing.T) {
	metrics := make(chan disruptor.PublishMetric, 1)

	rb, err := disruptor.NewRingBuffer(
		disruptor.EventFactoryFunc[longEvent](func() longEvent { return longEvent{} }),
		4,
		disruptor.WithMetricsSink(disruptor.MetricsSinkFunc{
			Publish: func(metric disruptor.PublishMetric) {
				metrics <- metric
			},
		}),
	)
	if err != nil {
		t.Fatalf("new ring buffer: %v", err)
	}

	err = rb.PublishEvent(context.Background(), disruptor.EventTranslatorFunc[longEvent](func(request disruptor.TranslateRequest[longEvent]) {
		request.Event.Value = 42
	}))
	if err != nil {
		t.Fatalf("publish event: %v", err)
	}

	select {
	case metric := <-metrics:
		if metric.Sequence != 0 {
			t.Fatalf("metric sequence = %d, want 0", metric.Sequence)
		}
		if metric.BatchSize != 1 {
			t.Fatalf("metric batch size = %d, want 1", metric.BatchSize)
		}
		if metric.Err != nil {
			t.Fatalf("metric err = %v, want nil", metric.Err)
		}
	default:
		t.Fatal("expected publish metric")
	}
}

func TestRingBufferWorksWithoutMetricsSink(t *testing.T) {
	rb := newTestRingBuffer(t, 4)

	err := rb.PublishEvent(context.Background(), disruptor.EventTranslatorFunc[longEvent](func(request disruptor.TranslateRequest[longEvent]) {
		request.Event.Value = 42
	}))
	if err != nil {
		t.Fatalf("publish event without metrics: %v", err)
	}
}
