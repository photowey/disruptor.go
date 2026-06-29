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

package ringbuffer

import (
	"github.com/photowey/disruptor.go/pkg/metrics"
	"github.com/photowey/disruptor.go/pkg/wait"
)

// ProducerType selects the producer sequencing strategy.
type ProducerType uint8

const (
	// ProducerTypeUnknown is the zero value and is rejected by configuration.
	ProducerTypeUnknown ProducerType = iota
	// ProducerTypeSingle uses the single-producer sequencer.
	ProducerTypeSingle
	// ProducerTypeMulti uses the multi-producer sequencer.
	ProducerTypeMulti
)

// Option configures ring buffer construction.
type Option func(config *options) error

type options struct {
	producerType ProducerType
	waitStrategy wait.Strategy
	metrics      metrics.Sink
}

// WithProducerType configures the ring buffer producer strategy.
func WithProducerType(producerType ProducerType) Option {
	return func(config *options) error {
		if producerType != ProducerTypeSingle && producerType != ProducerTypeMulti {
			return ErrInvalidSequence
		}

		config.producerType = producerType
		return nil
	}
}

// WithWaitStrategy configures the ring buffer wait strategy.
func WithWaitStrategy(strategy wait.Strategy) Option {
	return func(config *options) error {
		if strategy == nil {
			return ErrInvalidSequence
		}

		config.waitStrategy = strategy
		return nil
	}
}

// WithMetricsSink configures optional metrics collection.
func WithMetricsSink(sink metrics.Sink) Option {
	return func(config *options) error {
		config.metrics = sink
		return nil
	}
}

func defaultOptions() options {
	return options{
		producerType: ProducerTypeMulti,
		waitStrategy: wait.NewBlockingStrategy(),
		metrics:      nil,
	}
}

func producerTypeName(producerType ProducerType) string {
	switch producerType {
	case ProducerTypeSingle:
		return "single"
	case ProducerTypeMulti:
		return "multi"
	default:
		return "unknown"
	}
}
