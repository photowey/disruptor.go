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

import "context"

// EventFactory creates the initial value for each ring buffer slot.
type EventFactory[T any] interface {
	NewEvent() T
}

// EventFactoryFunc adapts a function to the EventFactory interface.
type EventFactoryFunc[T any] func() T

// NewEvent returns the event produced by the wrapped function.
func (fn EventFactoryFunc[T]) NewEvent() T {
	return fn()
}

// EventTranslator writes producer data into a claimed event slot.
type EventTranslator[T any] interface {
	Translate(request TranslateRequest[T])
}

// EventTranslatorFunc adapts a function to the EventTranslator interface.
type EventTranslatorFunc[T any] func(request TranslateRequest[T])

// Translate calls the wrapped translator function.
func (fn EventTranslatorFunc[T]) Translate(request TranslateRequest[T]) {
	fn(request)
}

// TranslateRequest carries the claimed event slot and sequence metadata.
type TranslateRequest[T any] struct {
	Context  context.Context
	Event    *T
	Sequence int64
}
