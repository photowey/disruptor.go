package disruptor_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/photowey/disruptor.go/pkg/disruptor"
)

func TestDisruptorFacadeStartsParallelConsumers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d, err := disruptor.New(
		disruptor.EventFactoryFunc[longEvent](func() longEvent { return longEvent{} }),
		8,
	)
	if err != nil {
		t.Fatalf("new disruptor: %v", err)
	}

	var mu sync.Mutex
	handledByConsumer := map[string][]int64{
		"a": {},
		"b": {},
	}
	done := make(chan struct{})

	handler := func(name string) disruptor.EventHandler[longEvent] {
		return disruptor.EventHandlerFunc[longEvent](func(request disruptor.EventRequest[longEvent]) error {
			mu.Lock()
			defer mu.Unlock()

			handledByConsumer[name] = append(handledByConsumer[name], request.Event.Value)
			if len(handledByConsumer["a"]) == 2 && len(handledByConsumer["b"]) == 2 {
				close(done)
			}

			return nil
		})
	}

	processors, err := d.HandleEventsWith(handler("a"), handler("b"))
	if err != nil {
		t.Fatalf("handle events with: %v", err)
	}
	if len(processors) != 2 {
		t.Fatalf("processor count = %d, want 2", len(processors))
	}

	if err := d.Start(ctx); err != nil {
		t.Fatalf("start disruptor: %v", err)
	}
	defer d.Stop()

	publishValues(t, d.RingBuffer(), 11, 22)
	waitForSignal(t, done)

	d.Stop()
	if err := d.Wait(); err != nil {
		t.Fatalf("wait disruptor: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	for _, name := range []string{"a", "b"} {
		values := handledByConsumer[name]
		if len(values) != 2 {
			t.Fatalf("consumer %s handled %d values, want 2", name, len(values))
		}
		if values[0] != 11 || values[1] != 22 {
			t.Fatalf("consumer %s values = %v, want [11 22]", name, values)
		}
	}
}

func TestDisruptorHandleEventsWithRequiresHandler(t *testing.T) {
	d, err := disruptor.New(
		disruptor.EventFactoryFunc[longEvent](func() longEvent { return longEvent{} }),
		8,
	)
	if err != nil {
		t.Fatalf("new disruptor: %v", err)
	}

	if _, err := d.HandleEventsWith(); err == nil {
		t.Fatal("expected error without handlers")
	}
}

func TestDisruptorWaitReturnsProcessorError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d, err := disruptor.New(
		disruptor.EventFactoryFunc[longEvent](func() longEvent { return longEvent{} }),
		8,
	)
	if err != nil {
		t.Fatalf("new disruptor: %v", err)
	}

	handlerErr := errDisruptorTestHandler
	_, err = d.HandleEventsWith(disruptor.EventHandlerFunc[longEvent](func(request disruptor.EventRequest[longEvent]) error {
		return handlerErr
	}))
	if err != nil {
		t.Fatalf("handle events with: %v", err)
	}
	if err := d.Start(ctx); err != nil {
		t.Fatalf("start disruptor: %v", err)
	}

	publishValues(t, d.RingBuffer(), 100)
	if err := d.Wait(); err == nil {
		t.Fatal("expected processor error")
	}
}

func TestDisruptorWaitStopsPeerProcessorsWhenOneFails(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d, err := disruptor.New(
		disruptor.EventFactoryFunc[longEvent](func() longEvent { return longEvent{} }),
		8,
	)
	if err != nil {
		t.Fatalf("new disruptor: %v", err)
	}

	handlerErr := errDisruptorTestHandler
	_, err = d.HandleEventsWith(
		disruptor.EventHandlerFunc[longEvent](func(request disruptor.EventRequest[longEvent]) error {
			return handlerErr
		}),
		disruptor.EventHandlerFunc[longEvent](func(request disruptor.EventRequest[longEvent]) error {
			return nil
		}),
	)
	if err != nil {
		t.Fatalf("handle events with: %v", err)
	}
	if err := d.Start(ctx); err != nil {
		t.Fatalf("start disruptor: %v", err)
	}

	publishValues(t, d.RingBuffer(), 100)

	waitDone := make(chan error, 1)
	go func() {
		waitDone <- d.Wait()
	}()

	select {
	case err := <-waitDone:
		if !errors.Is(err, handlerErr) {
			t.Fatalf("wait error = %v, want handler error", err)
		}
	case <-time.After(200 * time.Millisecond):
		d.Stop()
		<-waitDone
		t.Fatal("wait should stop peer processors after one processor fails")
	}
}

var errDisruptorTestHandler = &testHandlerError{}

type testHandlerError struct{}

func (e *testHandlerError) Error() string {
	return "handler failed"
}

func waitForDisruptorSignal(t *testing.T, done <-chan struct{}) {
	t.Helper()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for disruptor signal")
	}
}
