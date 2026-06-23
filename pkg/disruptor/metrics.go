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

type MetricsSink interface {
	OnPublish(request PublishMetric)
	OnBatchStart(request BatchMetric)
	OnEventHandled(request EventMetric)
	OnWait(request WaitMetric)
	OnProcessorState(request ProcessorMetric)
}

type PublishMetricFunc func(PublishMetric)
type BatchMetricFunc func(BatchMetric)
type EventMetricFunc func(EventMetric)
type WaitMetricFunc func(WaitMetric)
type ProcessorMetricFunc func(ProcessorMetric)

type MetricsSinkFunc struct {
	Publish        PublishMetricFunc
	BatchStart     BatchMetricFunc
	EventHandled   EventMetricFunc
	Wait           WaitMetricFunc
	ProcessorState ProcessorMetricFunc
}

func (f MetricsSinkFunc) OnPublish(request PublishMetric) {
	if f.Publish == nil {
		return
	}

	f.Publish(request)
}

func (f MetricsSinkFunc) OnBatchStart(request BatchMetric) {
	if f.BatchStart == nil {
		return
	}

	f.BatchStart(request)
}

func (f MetricsSinkFunc) OnEventHandled(request EventMetric) {
	if f.EventHandled == nil {
		return
	}

	f.EventHandled(request)
}

func (f MetricsSinkFunc) OnWait(request WaitMetric) {
	if f.Wait == nil {
		return
	}

	f.Wait(request)
}

func (f MetricsSinkFunc) OnProcessorState(request ProcessorMetric) {
	if f.ProcessorState == nil {
		return
	}

	f.ProcessorState(request)
}

type NoopMetricsSink struct{}

func (NoopMetricsSink) OnPublish(request PublishMetric)          {}
func (NoopMetricsSink) OnBatchStart(request BatchMetric)         {}
func (NoopMetricsSink) OnEventHandled(request EventMetric)       {}
func (NoopMetricsSink) OnWait(request WaitMetric)                {}
func (NoopMetricsSink) OnProcessorState(request ProcessorMetric) {}

type PublishMetric struct {
	ProducerType ProducerType
	Sequence     int64
	BatchSize    int64
	Duration     time.Duration
	Err          error
}

type BatchMetric struct {
	BatchSize  int64
	QueueDepth int64
}

type EventMetric struct {
	Sequence int64
	Duration time.Duration
	Err      error
}

type WaitMetric struct {
	RequestedSequence int64
	AvailableSequence int64
	Duration          time.Duration
	Strategy          string
	Err               error
}

type ProcessorMetric struct {
	State string
	Err   error
}
