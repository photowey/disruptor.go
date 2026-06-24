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

package event

import (
	"context"

	"github.com/photowey/disruptor.go/pkg/runtimevars"
)

// Handler consumes events produced by the ring buffer.
type Handler[T any] interface {
	OnEvent(request Request[T]) error
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc[T any] func(request Request[T]) error

// OnEvent calls the wrapped handler function.
func (fn HandlerFunc[T]) OnEvent(request Request[T]) error {
	return fn(request)
}

// Request provides the current event, sequence, and batch context.
type Request[T any] struct {
	Context    context.Context
	Event      *T
	Sequence   int64
	EndOfBatch bool
	Node       Node
	Runtime    runtimevars.ContextView
}

// BatchStartHandler is notified before a batch of events is processed.
type BatchStartHandler interface {
	OnBatchStart(request BatchStartRequest) error
}

// BatchStartRequest describes the batch that is about to be processed.
type BatchStartRequest struct {
	Context    context.Context
	BatchSize  int64
	QueueDepth int64
	Node       Node
}

// LifecycleHandler observes processor start and shutdown transitions.
type LifecycleHandler interface {
	OnStart(ctx context.Context) error
	OnShutdown(ctx context.Context) error
}
