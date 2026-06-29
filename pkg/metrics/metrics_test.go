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

package metrics_test

import (
	"context"
	"testing"
	"time"

	"github.com/photowey/disruptor.go/pkg/event"
	"github.com/photowey/disruptor.go/pkg/metrics"
	"github.com/photowey/disruptor.go/pkg/processor"
	"github.com/photowey/disruptor.go/pkg/ringbuffer"
)

func TestRingBufferReportsPublishMetricWhenSinkConfigured(t *testing.T) {
	publishMetrics := make(chan metrics.PublishMetric, 1)

	rb, err := ringbuffer.New(
		event.FactoryFunc[longEvent](func() longEvent { return longEvent{} }),
		4,
		ringbuffer.WithMetricsSink(metrics.SinkFunc{
			Publish: func(metric metrics.PublishMetric) {
				publishMetrics <- metric
			},
		}),
	)
	if err != nil {
		t.Fatalf("new ring buffer: %v", err)
	}

	err = rb.PublishEvent(context.Background(), event.TranslatorFunc[longEvent](func(request event.TranslateRequest[longEvent]) {
		request.Event.Value = 42
	}))
	if err != nil {
		t.Fatalf("publish event: %v", err)
	}

	select {
	case metric := <-publishMetrics:
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

	err := rb.PublishEvent(context.Background(), event.TranslatorFunc[longEvent](func(request event.TranslateRequest[longEvent]) {
		request.Event.Value = 42
	}))
	if err != nil {
		t.Fatalf("publish event without metrics: %v", err)
	}
}

func TestMetricsSinkFuncSupportsOptionalCallbacks(t *testing.T) {
	var sink metrics.Sink = metrics.SinkFunc{
		BatchStart: func(metric metrics.BatchMetric) {
			if metric.BatchSize != 2 {
				t.Fatalf("batch size = %d, want 2", metric.BatchSize)
			}
		},
		EventHandled: func(metric metrics.EventMetric) {
			if metric.Sequence != 3 {
				t.Fatalf("event sequence = %d, want 3", metric.Sequence)
			}
		},
		Wait: func(metric metrics.WaitMetric) {
			if metric.RequestedSequence != 5 {
				t.Fatalf("requested sequence = %d, want 5", metric.RequestedSequence)
			}
		},
		ProcessorState: func(metric metrics.ProcessorMetric) {
			if metric.State != "running" {
				t.Fatalf("processor state = %q, want running", metric.State)
			}
		},
	}

	sink.OnBatchStart(metrics.BatchMetric{BatchSize: 2})
	sink.OnEventHandled(metrics.EventMetric{Sequence: 3})
	sink.OnWait(metrics.WaitMetric{RequestedSequence: 5})
	sink.OnProcessorState(metrics.ProcessorMetric{State: "running"})
	sink.OnPublish(metrics.PublishMetric{})
}

func TestNoopMetricsSinkImplementsMetricsSink(t *testing.T) {
	var sink metrics.Sink = metrics.NoopSink{}

	sink.OnPublish(metrics.PublishMetric{})
	sink.OnBatchStart(metrics.BatchMetric{})
	sink.OnEventHandled(metrics.EventMetric{})
	sink.OnWait(metrics.WaitMetric{})
	sink.OnProcessorState(metrics.ProcessorMetric{})
}

func TestBatchEventProcessorReportsEventHandledMetric(t *testing.T) {
	events := make(chan metrics.EventMetric, 1)
	rb, err := ringbuffer.New(
		event.FactoryFunc[longEvent](func() longEvent { return longEvent{} }),
		4,
		ringbuffer.WithMetricsSink(metrics.SinkFunc{
			EventHandled: func(metric metrics.EventMetric) {
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
	processor, err := processor.NewBatchEventProcessor(rb, rb.NewBarrier(), handler)
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
	batches := make(chan metrics.BatchMetric, 1)
	rb, err := ringbuffer.New(
		event.FactoryFunc[longEvent](func() longEvent { return longEvent{} }),
		4,
		ringbuffer.WithMetricsSink(metrics.SinkFunc{
			BatchStart: func(metric metrics.BatchMetric) {
				batches <- metric
			},
		}),
	)
	if err != nil {
		t.Fatalf("new ring buffer: %v", err)
	}

	processor, err := processor.NewBatchEventProcessor(
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
	states := make(chan metrics.ProcessorMetric, 2)
	rb, err := ringbuffer.New(
		event.FactoryFunc[longEvent](func() longEvent { return longEvent{} }),
		4,
		ringbuffer.WithMetricsSink(metrics.SinkFunc{
			ProcessorState: func(metric metrics.ProcessorMetric) {
				states <- metric
			},
		}),
	)
	if err != nil {
		t.Fatalf("new ring buffer: %v", err)
	}

	processor, err := processor.NewBatchEventProcessor(
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
	waits := make(chan metrics.WaitMetric, 8)
	rb, err := ringbuffer.New(
		event.FactoryFunc[longEvent](func() longEvent { return longEvent{} }),
		4,
		ringbuffer.WithMetricsSink(metrics.SinkFunc{
			Wait: func(metric metrics.WaitMetric) {
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
	task := barrierWaitTask{
		ctx:     ctx,
		barrier: barrier,
		result:  errs,
	}
	go task.run()

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
	states <-chan metrics.ProcessorMetric,
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

type longEvent struct {
	Value int64
}

func newTestRingBuffer(t *testing.T, size int) *ringbuffer.RingBuffer[longEvent] {
	t.Helper()

	rb, err := ringbuffer.New(
		event.FactoryFunc[longEvent](func() longEvent { return longEvent{} }),
		size,
	)
	if err != nil {
		t.Fatalf("new ring buffer: %v", err)
	}

	return rb
}

func publishValues(t *testing.T, rb *ringbuffer.RingBuffer[longEvent], values ...int64) {
	t.Helper()

	ctx := context.Background()
	for _, value := range values {
		err := rb.PublishEvent(ctx, event.TranslatorFunc[longEvent](func(request event.TranslateRequest[longEvent]) {
			request.Event.Value = value
		}))
		if err != nil {
			t.Fatalf("publish event: %v", err)
		}
	}
}

type barrierWaitTask struct {
	ctx     context.Context
	barrier ringbuffer.Barrier
	result  chan<- error
}

func (task barrierWaitTask) run() {
	_, err := task.barrier.WaitFor(task.ctx, 0)
	task.result <- err
}
