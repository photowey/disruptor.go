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
	"context"
	"errors"
	"testing"
	"time"
)

func TestBatchEventProcessorReportsNodeContext(t *testing.T) {
	rb := newInternalProcessorRingBuffer(t, 4)
	requests := make(chan EventRequest[internalProcessorEvent], 1)
	handler := internalProcessorHandler{
		handle: func(request EventRequest[internalProcessorEvent]) error {
			requests <- request
			return nil
		},
	}

	processor, err := newBatchEventProcessor(
		rb,
		rb.NewBarrier(),
		handler,
		batchEventProcessorConfig[internalProcessorEvent]{
			exceptionHandler: NewFatalExceptionHandler[internalProcessorEvent](),
			producerGating:   true,
			haltAdvances:     true,
			node: NodeContext{
				GraphName: "orders",
				NodeName:  "persist",
				NodeLabel: "Persist",
			},
		},
	)
	if err != nil {
		t.Fatalf("new processor: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := processor.Start(ctx); err != nil {
		t.Fatalf("start processor: %v", err)
	}
	defer processor.Stop()

	publishInternalProcessorValue(t, rb, 7)

	select {
	case request := <-requests:
		if request.Node.GraphName != "orders" {
			t.Fatalf("graph name = %q, want orders", request.Node.GraphName)
		}
		if request.Node.NodeName != "persist" {
			t.Fatalf("node name = %q, want persist", request.Node.NodeName)
		}
		if request.Node.NodeLabel != "Persist" {
			t.Fatalf("node label = %q, want Persist", request.Node.NodeLabel)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timed out waiting for event request")
	}
}

func TestBatchEventProcessorReportsNodeContextForBatchStartException(t *testing.T) {
	rb := newInternalProcessorRingBuffer(t, 4)
	exceptions := make(chan LifecycleException, 1)
	handler := internalProcessorBatchStartHandler{
		batchStart: func(request BatchStartRequest) error {
			return errInternalProcessorBatchStartHandler
		},
		handle: func(request EventRequest[internalProcessorEvent]) error {
			return nil
		},
	}

	processor, err := newBatchEventProcessor(
		rb,
		rb.NewBarrier(),
		handler,
		batchEventProcessorConfig[internalProcessorEvent]{
			exceptionHandler: exceptionHandlerFunc[internalProcessorEvent]{
				handleEvent: func(request EventException[internalProcessorEvent]) ExceptionAction {
					return ExceptionActionHalt
				},
				handleStart: func(request LifecycleException) ExceptionAction {
					exceptions <- request
					return ExceptionActionContinue
				},
				handleShutdown: func(request LifecycleException) ExceptionAction {
					return ExceptionActionHalt
				},
			},
			producerGating: true,
			haltAdvances:   true,
			node: NodeContext{
				GraphName: "orders",
				NodeName:  "validate",
				NodeLabel: "Validate",
			},
		},
	)
	if err != nil {
		t.Fatalf("new processor: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := processor.Start(ctx); err != nil {
		t.Fatalf("start processor: %v", err)
	}
	defer processor.Stop()

	publishInternalProcessorValue(t, rb, 9)

	select {
	case request := <-exceptions:
		if request.Node.GraphName != "orders" {
			t.Fatalf("graph name = %q, want orders", request.Node.GraphName)
		}
		if request.Node.NodeName != "validate" {
			t.Fatalf("node name = %q, want validate", request.Node.NodeName)
		}
		if request.Node.NodeLabel != "Validate" {
			t.Fatalf("node label = %q, want Validate", request.Node.NodeLabel)
		}
		if !errors.Is(request.Err, errInternalProcessorBatchStartHandler) {
			t.Fatalf("request err = %v, want batch start handler error", request.Err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timed out waiting for batch start exception")
	}
}

func TestBatchEventProcessorCanSkipProducerGating(t *testing.T) {
	rb := newInternalProcessorRingBuffer(t, 1)
	_, err := newBatchEventProcessor(
		rb,
		rb.NewBarrier(),
		internalProcessorHandler{},
		batchEventProcessorConfig[internalProcessorEvent]{
			exceptionHandler: NewFatalExceptionHandler[internalProcessorEvent](),
			producerGating:   false,
			haltAdvances:     true,
		},
	)
	if err != nil {
		t.Fatalf("new processor: %v", err)
	}

	if _, err := rb.TryNext(); err != nil {
		t.Fatalf("first try next: %v", err)
	}
	if _, err := rb.TryNext(); err != nil {
		t.Fatalf("second try next should not be gated: %v", err)
	}
}

func TestBatchEventProcessorGraphHaltDoesNotAdvanceFailedSequence(t *testing.T) {
	rb := newInternalProcessorRingBuffer(t, 4)
	handlerErr := errInternalProcessorHandler
	processor, err := newBatchEventProcessor(
		rb,
		rb.NewBarrier(),
		internalProcessorHandler{
			handle: func(request EventRequest[internalProcessorEvent]) error {
				return handlerErr
			},
		},
		batchEventProcessorConfig[internalProcessorEvent]{
			exceptionHandler: NewFatalExceptionHandler[internalProcessorEvent](),
			producerGating:   false,
			haltAdvances:     false,
		},
	)
	if err != nil {
		t.Fatalf("new processor: %v", err)
	}

	if err := processor.Start(context.Background()); err != nil {
		t.Fatalf("start processor: %v", err)
	}
	publishInternalProcessorValue(t, rb, 9)

	if err := processor.Wait(); !errors.Is(err, handlerErr) {
		t.Fatalf("wait error = %v, want handler error", err)
	}
	if got := processor.Sequence().Value(); got != InitialSequenceValue {
		t.Fatalf("sequence = %d, want initial value", got)
	}
}

func TestBatchEventProcessorPublicConstructorKeepsV1HaltAdvance(t *testing.T) {
	rb := newInternalProcessorRingBuffer(t, 4)
	handlerErr := errInternalProcessorHandler
	processor, err := NewBatchEventProcessor(
		rb,
		rb.NewBarrier(),
		internalProcessorHandler{
			handle: func(request EventRequest[internalProcessorEvent]) error {
				return handlerErr
			},
		},
	)
	if err != nil {
		t.Fatalf("new processor: %v", err)
	}

	if err := processor.Start(context.Background()); err != nil {
		t.Fatalf("start processor: %v", err)
	}
	publishInternalProcessorValue(t, rb, 11)

	if err := processor.Wait(); !errors.Is(err, handlerErr) {
		t.Fatalf("wait error = %v, want handler error", err)
	}
	if got := processor.Sequence().Value(); got != 0 {
		t.Fatalf("sequence = %d, want failed sequence to advance", got)
	}
}

type internalProcessorBatchStartHandler struct {
	batchStart func(BatchStartRequest) error
	handle     func(EventRequest[internalProcessorEvent]) error
}

func (h internalProcessorBatchStartHandler) OnBatchStart(
	request BatchStartRequest,
) error {
	if h.batchStart == nil {
		return nil
	}

	return h.batchStart(request)
}

func (h internalProcessorBatchStartHandler) OnEvent(
	request EventRequest[internalProcessorEvent],
) error {
	if h.handle == nil {
		return nil
	}

	return h.handle(request)
}

func newInternalProcessorRingBuffer(
	t *testing.T,
	size int,
) *RingBuffer[internalProcessorEvent] {
	t.Helper()

	rb, err := NewRingBuffer(
		EventFactoryFunc[internalProcessorEvent](func() internalProcessorEvent {
			return internalProcessorEvent{}
		}),
		size,
	)
	if err != nil {
		t.Fatalf("new ring buffer: %v", err)
	}

	return rb
}

func publishInternalProcessorValue(
	t *testing.T,
	rb *RingBuffer[internalProcessorEvent],
	value int64,
) {
	t.Helper()

	err := rb.PublishEvent(
		context.Background(),
		EventTranslatorFunc[internalProcessorEvent](func(
			request TranslateRequest[internalProcessorEvent],
		) {
			request.Event.Value = value
		}),
	)
	if err != nil {
		t.Fatalf("publish event: %v", err)
	}
}

type internalProcessorEvent struct {
	Value int64
}

type internalProcessorHandler struct {
	handle func(EventRequest[internalProcessorEvent]) error
}

func (h internalProcessorHandler) OnEvent(
	request EventRequest[internalProcessorEvent],
) error {
	if h.handle == nil {
		return nil
	}

	return h.handle(request)
}

var errInternalProcessorHandler = errors.New("internal processor handler failed")
var errInternalProcessorBatchStartHandler = errors.New("internal processor batch start failed")
