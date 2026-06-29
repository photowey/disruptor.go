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

package metrics

import (
	"time"

	"github.com/photowey/disruptor.go/pkg/event"
)

// Sink receives optional instrumentation events.
type Sink interface {
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

// ProcessorMetricFunc adapts a function to processor state metrics.
type ProcessorMetricFunc func(ProcessorMetric)

// SinkFunc groups optional metric callbacks.
type SinkFunc struct {
	Publish        PublishMetricFunc
	BatchStart     BatchMetricFunc
	EventHandled   EventMetricFunc
	Wait           WaitMetricFunc
	ProcessorState ProcessorMetricFunc
}

// OnPublish reports a publish metric when configured.
func (f SinkFunc) OnPublish(request PublishMetric) {
	if f.Publish != nil {
		f.Publish(request)
	}
}

// OnBatchStart reports a batch metric when configured.
func (f SinkFunc) OnBatchStart(request BatchMetric) {
	if f.BatchStart != nil {
		f.BatchStart(request)
	}
}

// OnEventHandled reports an event metric when configured.
func (f SinkFunc) OnEventHandled(request EventMetric) {
	if f.EventHandled != nil {
		f.EventHandled(request)
	}
}

// OnWait reports a wait metric when configured.
func (f SinkFunc) OnWait(request WaitMetric) {
	if f.Wait != nil {
		f.Wait(request)
	}
}

// OnProcessorState reports a processor state metric when configured.
func (f SinkFunc) OnProcessorState(request ProcessorMetric) {
	if f.ProcessorState != nil {
		f.ProcessorState(request)
	}
}

// NoopSink discards all metrics.
type NoopSink struct{}

// OnPublish discards publish metrics.
func (NoopSink) OnPublish(PublishMetric) {}

// OnBatchStart discards batch metrics.
func (NoopSink) OnBatchStart(BatchMetric) {}

// OnEventHandled discards event metrics.
func (NoopSink) OnEventHandled(EventMetric) {}

// OnWait discards wait metrics.
func (NoopSink) OnWait(WaitMetric) {}

// OnProcessorState discards processor state metrics.
func (NoopSink) OnProcessorState(ProcessorMetric) {}

// PublishMetric describes a producer publish operation.
type PublishMetric struct {
	ProducerType string
	Sequence     int64
	BatchSize    int64
	Duration     time.Duration
	Err          error
}

// BatchMetric describes the start of a batch.
type BatchMetric struct {
	BatchSize  int64
	QueueDepth int64
	Node       event.Node
}

// EventMetric describes a handled event.
type EventMetric struct {
	Sequence int64
	Duration time.Duration
	Err      error
	Node     event.Node
}

// WaitMetric describes a barrier or capacity wait.
type WaitMetric struct {
	RequestedSequence int64
	AvailableSequence int64
	Duration          time.Duration
	Strategy          string
	Err               error
}

// ProcessorMetric describes processor lifecycle state.
type ProcessorMetric struct {
	State string
	Err   error
	Node  event.Node
}
