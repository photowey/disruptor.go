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

type ProducerType uint8

const (
	ProducerTypeUnknown ProducerType = iota
	ProducerTypeSingle
	ProducerTypeMulti
)

type RingBufferOption interface {
	applyRingBuffer(config *ringBufferConfig) error
}

type ringBufferConfig struct {
	producerType ProducerType
	waitStrategy WaitStrategy
	metrics      MetricsSink
}

type ringBufferOptionFunc struct {
	applyFunc ringBufferApplyFunc
}

type ringBufferApplyFunc func(config *ringBufferConfig) error

func (fn ringBufferOptionFunc) applyRingBuffer(config *ringBufferConfig) error {
	return fn.applyFunc(config)
}

func WithProducerType(producerType ProducerType) RingBufferOption {
	return ringBufferOptionFunc{
		applyFunc: func(config *ringBufferConfig) error {
			if producerType != ProducerTypeSingle && producerType != ProducerTypeMulti {
				return ErrInvalidSequence
			}

			config.producerType = producerType
			return nil
		},
	}
}

func WithWaitStrategy(strategy WaitStrategy) RingBufferOption {
	return ringBufferOptionFunc{
		applyFunc: func(config *ringBufferConfig) error {
			if strategy == nil {
				return ErrInvalidSequence
			}

			config.waitStrategy = strategy
			return nil
		},
	}
}

func WithMetricsSink(sink MetricsSink) RingBufferOption {
	return ringBufferOptionFunc{
		applyFunc: func(config *ringBufferConfig) error {
			if sink == nil {
				config.metrics = nil
				return nil
			}

			config.metrics = sink
			return nil
		},
	}
}

func defaultRingBufferConfig() ringBufferConfig {
	return ringBufferConfig{
		producerType: ProducerTypeMulti,
		waitStrategy: NewBlockingWaitStrategy(),
		metrics:      nil,
	}
}
