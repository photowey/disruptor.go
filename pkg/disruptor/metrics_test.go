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

package disruptor_test

import (
	"context"
	"testing"
	"time"

	"github.com/photowey/disruptor.go/pkg/disruptor"
	"github.com/photowey/disruptor.go/pkg/event"
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
		if metric.Duration <= 0 {
			t.Fatalf("metric duration = %s, want positive duration", metric.Duration)
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

func TestMetricsSinkFuncSupportsOptionalCallbacks(t *testing.T) {
	var sink disruptor.MetricsSink = disruptor.MetricsSinkFunc{
		BatchStart: func(metric disruptor.BatchMetric) {
			if metric.BatchSize != 2 {
				t.Fatalf("batch size = %d, want 2", metric.BatchSize)
			}
		},
		EventHandled: func(metric disruptor.EventMetric) {
			if metric.Sequence != 3 {
				t.Fatalf("event sequence = %d, want 3", metric.Sequence)
			}
		},
		Wait: func(metric disruptor.WaitMetric) {
			if metric.RequestedSequence != 5 {
				t.Fatalf("requested sequence = %d, want 5", metric.RequestedSequence)
			}
		},
		ProcessorState: func(metric disruptor.ProcessorMetric) {
			if metric.State != "running" {
				t.Fatalf("processor state = %q, want running", metric.State)
			}
		},
	}

	sink.OnBatchStart(disruptor.BatchMetric{BatchSize: 2})
	sink.OnEventHandled(disruptor.EventMetric{Sequence: 3})
	sink.OnWait(disruptor.WaitMetric{RequestedSequence: 5})
	sink.OnProcessorState(disruptor.ProcessorMetric{State: "running"})
	sink.OnPublish(disruptor.PublishMetric{})
}

func TestNoopMetricsSinkImplementsMetricsSink(t *testing.T) {
	var sink disruptor.MetricsSink = disruptor.NoopMetricsSink{}

	sink.OnPublish(disruptor.PublishMetric{})
	sink.OnBatchStart(disruptor.BatchMetric{})
	sink.OnEventHandled(disruptor.EventMetric{})
	sink.OnWait(disruptor.WaitMetric{})
	sink.OnProcessorState(disruptor.ProcessorMetric{})
}

func TestBatchEventProcessorReportsEventHandledMetric(t *testing.T) {
	events := make(chan disruptor.EventMetric, 1)
	rb, err := disruptor.NewRingBuffer(
		disruptor.EventFactoryFunc[longEvent](func() longEvent { return longEvent{} }),
		4,
		disruptor.WithMetricsSink(disruptor.MetricsSinkFunc{
			EventHandled: func(metric disruptor.EventMetric) {
				events <- metric
			},
		}),
	)
	if err != nil {
		t.Fatalf("new ring buffer: %v", err)
	}

	handler := event.HandlerFunc[longEvent](func(request event.Request[longEvent]) error {
		return nil
	})
	processor, err := disruptor.NewBatchEventProcessor(rb, rb.NewBarrier(), handler)
	if err != nil {
		t.Fatalf("new processor: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := processor.Start(ctx); err != nil {
		t.Fatalf("start processor: %v", err)
	}
	defer processor.Stop()

	publishValues(t, rb, 42)

	select {
	case metric := <-events:
		if metric.Sequence != 0 {
			t.Fatalf("metric sequence = %d, want 0", metric.Sequence)
		}
		if metric.Err != nil {
			t.Fatalf("metric err = %v, want nil", metric.Err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event metric")
	}
}

func TestBatchEventProcessorReportsBatchStartMetric(t *testing.T) {
	batches := make(chan disruptor.BatchMetric, 1)
	rb, err := disruptor.NewRingBuffer(
		disruptor.EventFactoryFunc[longEvent](func() longEvent { return longEvent{} }),
		4,
		disruptor.WithMetricsSink(disruptor.MetricsSinkFunc{
			BatchStart: func(metric disruptor.BatchMetric) {
				batches <- metric
			},
		}),
	)
	if err != nil {
		t.Fatalf("new ring buffer: %v", err)
	}

	processor, err := disruptor.NewBatchEventProcessor(
		rb,
		rb.NewBarrier(),
		event.HandlerFunc[longEvent](func(request event.Request[longEvent]) error {
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("new processor: %v", err)
	}

	ctx := context.Background()
	hi, err := rb.NextN(ctx, 2)
	if err != nil {
		t.Fatalf("next batch: %v", err)
	}
	lo := hi - 1
	rb.PublishRange(lo, hi)

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := processor.Start(runCtx); err != nil {
		t.Fatalf("start processor: %v", err)
	}
	defer processor.Stop()

	select {
	case metric := <-batches:
		if metric.BatchSize != 2 {
			t.Fatalf("batch size = %d, want 2", metric.BatchSize)
		}
		if metric.QueueDepth != 2 {
			t.Fatalf("queue depth = %d, want 2", metric.QueueDepth)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for batch metric")
	}
}

func TestBatchEventProcessorReportsProcessorStateMetrics(t *testing.T) {
	states := make(chan disruptor.ProcessorMetric, 2)
	rb, err := disruptor.NewRingBuffer(
		disruptor.EventFactoryFunc[longEvent](func() longEvent { return longEvent{} }),
		4,
		disruptor.WithMetricsSink(disruptor.MetricsSinkFunc{
			ProcessorState: func(metric disruptor.ProcessorMetric) {
				states <- metric
			},
		}),
	)
	if err != nil {
		t.Fatalf("new ring buffer: %v", err)
	}

	processor, err := disruptor.NewBatchEventProcessor(
		rb,
		rb.NewBarrier(),
		event.HandlerFunc[longEvent](func(request event.Request[longEvent]) error {
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("new processor: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := processor.Start(ctx); err != nil {
		t.Fatalf("start processor: %v", err)
	}

	assertProcessorState(t, states, "running")
	processor.Stop()
	if err := processor.Wait(); err != nil {
		t.Fatalf("wait processor: %v", err)
	}
	assertProcessorState(t, states, "stopped")
}

func TestBarrierReportsWaitMetric(t *testing.T) {
	waits := make(chan disruptor.WaitMetric, 8)
	rb, err := disruptor.NewRingBuffer(
		disruptor.EventFactoryFunc[longEvent](func() longEvent { return longEvent{} }),
		4,
		disruptor.WithMetricsSink(disruptor.MetricsSinkFunc{
			Wait: func(metric disruptor.WaitMetric) {
				waits <- metric
			},
		}),
	)
	if err != nil {
		t.Fatalf("new ring buffer: %v", err)
	}

	barrier := rb.NewBarrier()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errs := make(chan error, 1)
	go func() {
		_, waitErr := barrier.WaitFor(ctx, 0)
		errs <- waitErr
	}()

	select {
	case metric := <-waits:
		t.Fatalf("wait metric reported before signal: %+v", metric)
	case <-time.After(10 * time.Millisecond):
	}

	publishValues(t, rb, 1)

	select {
	case err := <-errs:
		if err != nil {
			t.Fatalf("barrier wait: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for barrier")
	}

	select {
	case metric := <-waits:
		if metric.RequestedSequence != 0 {
			t.Fatalf("requested sequence = %d, want 0", metric.RequestedSequence)
		}
		if metric.AvailableSequence != 0 {
			t.Fatalf("available sequence = %d, want 0", metric.AvailableSequence)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for wait metric")
	}
}

func assertProcessorState(
	t *testing.T,
	states <-chan disruptor.ProcessorMetric,
	expected string,
) {
	t.Helper()

	select {
	case metric := <-states:
		if metric.State != expected {
			t.Fatalf("processor state = %q, want %q", metric.State, expected)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for processor state %q", expected)
	}
}
