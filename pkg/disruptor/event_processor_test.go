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
