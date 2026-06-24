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

import (
	"fmt"

	"github.com/photowey/disruptor.go/pkg/event"
)

// ProcessorOption configures a batch event processor.
type ProcessorOption[T any] interface {
	applyProcessor(config *processorConfig[T]) error
}

type processorConfig[T any] struct {
	exceptionHandler event.ExceptionHandler[T]
}

type processorOptionFunc[T any] struct {
	applyFunc func(*processorConfig[T]) error
}

//nolint:unused // The method satisfies ProcessorOption[T] and is called through the interface.
func (fn processorOptionFunc[T]) applyProcessor(config *processorConfig[T]) error {
	return fn.applyFunc(config)
}

// WithExceptionHandler sets the processor exception handler.
func WithExceptionHandler[T any](handler event.ExceptionHandler[T]) ProcessorOption[T] {
	return processorOptionFunc[T]{
		applyFunc: func(config *processorConfig[T]) error {
			if handler == nil {
				return fmt.Errorf("disruptor: exception handler is nil")
			}

			config.exceptionHandler = handler
			return nil
		},
	}
}

func defaultProcessorConfig[T any]() processorConfig[T] {
	return processorConfig[T]{
		exceptionHandler: event.NewFatalExceptionHandler[T](),
	}
}
