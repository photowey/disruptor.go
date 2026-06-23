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

package disruptor_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/photowey/disruptor.go/pkg/disruptor"
)

func TestBatchEventProcessorHandlesPublishedEventsAndAdvancesSequence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rb := newTestRingBuffer(t, 8)
	done := make(chan struct{})

	var mu sync.Mutex
	handled := make([]int64, 0, 3)
	handler := disruptor.EventHandlerFunc[longEvent](func(request disruptor.EventRequest[longEvent]) error {
		mu.Lock()
		handled = append(handled, request.Event.Value)
		if request.Sequence == 2 {
			close(done)
		}
		mu.Unlock()

		return nil
	})

	processor, err := disruptor.NewBatchEventProcessor(
		rb,
		rb.NewBarrier(),
		handler,
	)
	if err != nil {
		t.Fatalf("new processor: %v", err)
	}

	if err := processor.Start(ctx); err != nil {
		t.Fatalf("start processor: %v", err)
	}
	defer processor.Stop()

	publishValues(t, rb, 1, 2, 3)
	waitForSignal(t, done)

	processor.Stop()
	if err := processor.Wait(); err != nil {
		t.Fatalf("wait processor: %v", err)
	}
	if got := processor.Sequence().Value(); got != 2 {
		t.Fatalf("processor sequence = %d, want 2", got)
	}

	mu.Lock()
	defer mu.Unlock()
	expected := []int64{1, 2, 3}
	if len(handled) != len(expected) {
		t.Fatalf("handled length = %d, want %d", len(handled), len(expected))
	}
	for i, value := range expected {
		if handled[i] != value {
			t.Fatalf("handled[%d] = %d, want %d", i, handled[i], value)
		}
	}
}

func TestBatchEventProcessorContinuesWhenExceptionHandlerRequestsContinue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rb := newTestRingBuffer(t, 8)
	handlerErr := errors.New("handler failed")
	done := make(chan struct{})

	handler := disruptor.EventHandlerFunc[longEvent](func(request disruptor.EventRequest[longEvent]) error {
		if request.Sequence == 0 {
			return handlerErr
		}
		if request.Sequence == 1 {
			close(done)
		}

		return nil
	})

	exceptions := make(chan disruptor.EventException[longEvent], 1)
	processor, err := disruptor.NewBatchEventProcessor(
		rb,
		rb.NewBarrier(),
		handler,
		disruptor.WithExceptionHandler[longEvent](continueExceptionHandler[longEvent]{
			events: exceptions,
		}),
	)
	if err != nil {
		t.Fatalf("new processor: %v", err)
	}

	if err := processor.Start(ctx); err != nil {
		t.Fatalf("start processor: %v", err)
	}
	defer processor.Stop()

	publishValues(t, rb, 10, 20)
	waitForSignal(t, done)

	processor.Stop()
	if err := processor.Wait(); err != nil {
		t.Fatalf("wait processor: %v", err)
	}
	if got := processor.Sequence().Value(); got != 1 {
		t.Fatalf("processor sequence = %d, want 1", got)
	}

	select {
	case exception := <-exceptions:
		if !errors.Is(exception.Err, handlerErr) {
			t.Fatalf("exception error = %v, want handler error", exception.Err)
		}
		if exception.Sequence != 0 {
			t.Fatalf("exception sequence = %d, want 0", exception.Sequence)
		}
	default:
		t.Fatal("expected event exception")
	}
}

func TestBatchEventProcessorDefaultHandlerHaltsOnError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rb := newTestRingBuffer(t, 4)
	handlerErr := errors.New("handler failed")
	handler := disruptor.EventHandlerFunc[longEvent](func(request disruptor.EventRequest[longEvent]) error {
		return handlerErr
	})

	processor, err := disruptor.NewBatchEventProcessor(rb, rb.NewBarrier(), handler)
	if err != nil {
		t.Fatalf("new processor: %v", err)
	}
	if err := processor.Start(ctx); err != nil {
		t.Fatalf("start processor: %v", err)
	}

	publishValues(t, rb, 99)

	if err := processor.Wait(); !errors.Is(err, handlerErr) {
		t.Fatalf("wait error = %v, want handler error", err)
	}
	if got := processor.Sequence().Value(); got != 0 {
		t.Fatalf("processor sequence = %d, want failed sequence stored", got)
	}
}

func TestRetryExceptionHandlerRetriesUntilSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rb := newTestRingBuffer(t, 4)
	handlerErr := errors.New("handler failed")
	done := make(chan struct{})

	var attempts int
	handler := disruptor.EventHandlerFunc[longEvent](func(request disruptor.EventRequest[longEvent]) error {
		attempts++
		if attempts <= 2 {
			return handlerErr
		}

		close(done)
		return nil
	})

	retryHandler, err := disruptor.NewRetryExceptionHandler[longEvent](2, disruptor.ExceptionActionHalt)
	if err != nil {
		t.Fatalf("new retry exception handler: %v", err)
	}
	processor, err := disruptor.NewBatchEventProcessor(
		rb,
		rb.NewBarrier(),
		handler,
		disruptor.WithExceptionHandler[longEvent](retryHandler),
	)
	if err != nil {
		t.Fatalf("new processor: %v", err)
	}

	if err := processor.Start(ctx); err != nil {
		t.Fatalf("start processor: %v", err)
	}
	defer processor.Stop()

	publishValues(t, rb, 1)
	waitForSignal(t, done)

	processor.Stop()
	if err := processor.Wait(); err != nil {
		t.Fatalf("wait processor: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
}

func TestRetryExceptionHandlerHaltsAfterMaxRetries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rb := newTestRingBuffer(t, 4)
	handlerErr := errors.New("handler failed")

	var attempts int
	handler := disruptor.EventHandlerFunc[longEvent](func(request disruptor.EventRequest[longEvent]) error {
		attempts++
		return handlerErr
	})

	retryHandler, err := disruptor.NewRetryExceptionHandler[longEvent](1, disruptor.ExceptionActionHalt)
	if err != nil {
		t.Fatalf("new retry exception handler: %v", err)
	}
	processor, err := disruptor.NewBatchEventProcessor(
		rb,
		rb.NewBarrier(),
		handler,
		disruptor.WithExceptionHandler[longEvent](retryHandler),
	)
	if err != nil {
		t.Fatalf("new processor: %v", err)
	}
	if err := processor.Start(ctx); err != nil {
		t.Fatalf("start processor: %v", err)
	}

	publishValues(t, rb, 1)
	if err := processor.Wait(); !errors.Is(err, handlerErr) {
		t.Fatalf("wait error = %v, want handler error", err)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
}

func TestBatchEventProcessorRemovesGatingSequenceOnExit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rb := newTestRingBuffer(t, 1)
	handler := disruptor.EventHandlerFunc[longEvent](func(request disruptor.EventRequest[longEvent]) error {
		return nil
	})

	processor, err := disruptor.NewBatchEventProcessor(rb, rb.NewBarrier(), handler)
	if err != nil {
		t.Fatalf("new processor: %v", err)
	}
	if err := processor.Start(ctx); err != nil {
		t.Fatalf("start processor: %v", err)
	}

	processor.Stop()
	if err := processor.Wait(); err != nil {
		t.Fatalf("wait processor: %v", err)
	}

	sequence, err := rb.TryNext()
	if err != nil {
		t.Fatalf("first try next after processor stop: %v", err)
	}
	rb.Publish(sequence)

	if _, err := rb.TryNext(); err != nil {
		t.Fatalf("second try next after processor stop should not be gated: %v", err)
	}
}

func TestBatchEventProcessorSignalsCapacityWaitersAfterAdvancing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rb := newTestRingBuffer(t, 1)
	entered := make(chan struct{})
	release := make(chan struct{})
	handler := disruptor.EventHandlerFunc[longEvent](func(
		request disruptor.EventRequest[longEvent],
	) error {
		close(entered)
		<-release
		return nil
	})
	processor, err := disruptor.NewBatchEventProcessor(rb, rb.NewBarrier(), handler)
	if err != nil {
		t.Fatalf("new processor: %v", err)
	}
	if err := processor.Start(ctx); err != nil {
		t.Fatalf("start processor: %v", err)
	}
	defer processor.Stop()

	publishValues(t, rb, 1)
	waitForSignal(t, entered)

	nextResult := make(chan error, 1)
	go func() {
		_, err := rb.Next(ctx)
		nextResult <- err
	}()

	select {
	case err := <-nextResult:
		t.Fatalf("next returned before consumer advanced: %v", err)
	case <-time.After(10 * time.Millisecond):
	}

	close(release)

	select {
	case err := <-nextResult:
		if err != nil {
			t.Fatalf("next after consumer advance: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for capacity waiter")
	}
}

func TestBatchEventProcessorInvokesBatchStartHandler(t *testing.T) {
	rb := newTestRingBuffer(t, 8)
	handler := &batchStartRecordingHandler{
		batches: make(chan disruptor.BatchStartRequest, 1),
		events:  make(chan int64, 3),
	}
	processor, err := disruptor.NewBatchEventProcessor(rb, rb.NewBarrier(), handler)
	if err != nil {
		t.Fatalf("new processor: %v", err)
	}

	const batchSize int64 = 3
	hi, err := rb.NextN(context.Background(), batchSize)
	if err != nil {
		t.Fatalf("next batch: %v", err)
	}
	lo := hi - batchSize + 1
	for sequence := lo; sequence <= hi; sequence++ {
		rb.Get(sequence).Value = sequence
	}
	rb.PublishRange(lo, hi)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := processor.Start(ctx); err != nil {
		t.Fatalf("start processor: %v", err)
	}
	defer processor.Stop()

	select {
	case request := <-handler.batches:
		if request.Context == nil {
			t.Fatal("batch start context should be set")
		}
		if request.BatchSize != batchSize {
			t.Fatalf("batch size = %d, want %d", request.BatchSize, batchSize)
		}
		if request.QueueDepth != batchSize {
			t.Fatalf("queue depth = %d, want %d", request.QueueDepth, batchSize)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for batch start")
	}
}

func TestBatchEventProcessorInvokesLifecycleHandler(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rb := newTestRingBuffer(t, 4)
	handler := &lifecycleRecordingHandler{
		started:  make(chan struct{}, 1),
		shutdown: make(chan struct{}, 1),
	}
	processor, err := disruptor.NewBatchEventProcessor(rb, rb.NewBarrier(), handler)
	if err != nil {
		t.Fatalf("new processor: %v", err)
	}
	if err := processor.Start(ctx); err != nil {
		t.Fatalf("start processor: %v", err)
	}

	waitForSignal(t, handler.started)
	processor.Stop()
	if err := processor.Wait(); err != nil {
		t.Fatalf("wait processor: %v", err)
	}
	waitForSignal(t, handler.shutdown)
}

type batchStartRecordingHandler struct {
	batches chan disruptor.BatchStartRequest
	events  chan int64
}

func (h *batchStartRecordingHandler) OnBatchStart(
	request disruptor.BatchStartRequest,
) error {
	h.batches <- request
	return nil
}

func (h *batchStartRecordingHandler) OnEvent(
	request disruptor.EventRequest[longEvent],
) error {
	h.events <- request.Event.Value
	return nil
}

type lifecycleRecordingHandler struct {
	started  chan struct{}
	shutdown chan struct{}
}

func (h *lifecycleRecordingHandler) OnStart(ctx context.Context) error {
	h.started <- struct{}{}
	return nil
}

func (h *lifecycleRecordingHandler) OnShutdown(ctx context.Context) error {
	h.shutdown <- struct{}{}
	return nil
}

func (h *lifecycleRecordingHandler) OnEvent(
	request disruptor.EventRequest[longEvent],
) error {
	return nil
}

type continueExceptionHandler[T any] struct {
	events chan<- disruptor.EventException[T]
}

func (h continueExceptionHandler[T]) HandleEventException(
	request disruptor.EventException[T],
) disruptor.ExceptionAction {
	h.events <- request
	return disruptor.ExceptionActionContinue
}

func (h continueExceptionHandler[T]) HandleStartException(
	request disruptor.LifecycleException,
) disruptor.ExceptionAction {
	return disruptor.ExceptionActionHalt
}

func (h continueExceptionHandler[T]) HandleShutdownException(
	request disruptor.LifecycleException,
) disruptor.ExceptionAction {
	return disruptor.ExceptionActionHalt
}

func publishValues(t *testing.T, rb *disruptor.RingBuffer[longEvent], values ...int64) {
	t.Helper()

	ctx := context.Background()
	for _, value := range values {
		err := rb.PublishEvent(ctx, disruptor.EventTranslatorFunc[longEvent](func(request disruptor.TranslateRequest[longEvent]) {
			request.Event.Value = value
		}))
		if err != nil {
			t.Fatalf("publish event: %v", err)
		}
	}
}

func waitForSignal(t *testing.T, done <-chan struct{}) {
	t.Helper()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for signal")
	}
}
