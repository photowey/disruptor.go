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

package disruptor

import "time"

// MetricsSink receives backend-neutral runtime metrics.
type MetricsSink interface {
	OnPublish(request PublishMetric)
	OnBatchStart(request BatchMetric)
	OnEventHandled(request EventMetric)
	OnWait(request WaitMetric)
	OnProcessorState(request ProcessorMetric)
}

// PublishMetricFunc adapts a function to publish metrics.
type PublishMetricFunc func(PublishMetric)

// BatchMetricFunc adapts a function to batch metrics.
type BatchMetricFunc func(BatchMetric)

// EventMetricFunc adapts a function to event metrics.
type EventMetricFunc func(EventMetric)

// WaitMetricFunc adapts a function to wait metrics.
type WaitMetricFunc func(WaitMetric)

// ProcessorMetricFunc adapts a function to processor metrics.
type ProcessorMetricFunc func(ProcessorMetric)

// MetricsSinkFunc combines optional metric callbacks into one sink.
type MetricsSinkFunc struct {
	Publish        PublishMetricFunc
	BatchStart     BatchMetricFunc
	EventHandled   EventMetricFunc
	Wait           WaitMetricFunc
	ProcessorState ProcessorMetricFunc
}

// OnPublish dispatches the publish callback when configured.
func (f MetricsSinkFunc) OnPublish(request PublishMetric) {
	if f.Publish == nil {
		return
	}

	f.Publish(request)
}

// OnBatchStart dispatches the batch-start callback when configured.
func (f MetricsSinkFunc) OnBatchStart(request BatchMetric) {
	if f.BatchStart == nil {
		return
	}

	f.BatchStart(request)
}

// OnEventHandled dispatches the event-handled callback when configured.
func (f MetricsSinkFunc) OnEventHandled(request EventMetric) {
	if f.EventHandled == nil {
		return
	}

	f.EventHandled(request)
}

// OnWait dispatches the wait callback when configured.
func (f MetricsSinkFunc) OnWait(request WaitMetric) {
	if f.Wait == nil {
		return
	}

	f.Wait(request)
}

// OnProcessorState dispatches the processor-state callback when configured.
func (f MetricsSinkFunc) OnProcessorState(request ProcessorMetric) {
	if f.ProcessorState == nil {
		return
	}

	f.ProcessorState(request)
}

// NoopMetricsSink implements MetricsSink without recording anything.
type NoopMetricsSink struct{}

func (NoopMetricsSink) OnPublish(request PublishMetric)          {}
func (NoopMetricsSink) OnBatchStart(request BatchMetric)         {}
func (NoopMetricsSink) OnEventHandled(request EventMetric)       {}
func (NoopMetricsSink) OnWait(request WaitMetric)                {}
func (NoopMetricsSink) OnProcessorState(request ProcessorMetric) {}

// PublishMetric describes a publish operation.
type PublishMetric struct {
	ProducerType ProducerType
	Sequence     int64
	BatchSize    int64
	Duration     time.Duration
	Err          error
}

// BatchMetric describes a batch start notification.
type BatchMetric struct {
	BatchSize  int64
	QueueDepth int64
	Node       NodeContext
}

// EventMetric describes a handled event.
type EventMetric struct {
	Sequence int64
	Duration time.Duration
	Err      error
	Node     NodeContext
}

// WaitMetric describes a wait operation.
type WaitMetric struct {
	RequestedSequence int64
	AvailableSequence int64
	Duration          time.Duration
	Strategy          string
	Err               error
}

// ProcessorMetric describes a processor lifecycle event.
type ProcessorMetric struct {
	State string
	Err   error
	Node  NodeContext
}
