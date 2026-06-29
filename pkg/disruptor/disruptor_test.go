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
	"github.com/photowey/disruptor.go/pkg/event"
)

func TestDisruptorFacadeStartsParallelConsumers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d, err := disruptor.New(
		event.FactoryFunc[longEvent](func() longEvent { return longEvent{} }),
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

	handler := func(name string) event.Handler[longEvent] {
		return event.HandlerFunc[longEvent](func(request event.Request[longEvent]) error {
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
		event.FactoryFunc[longEvent](func() longEvent { return longEvent{} }),
		8,
	)
	if err != nil {
		t.Fatalf("new disruptor: %v", err)
	}

	if _, err := d.HandleEventsWith(); err == nil {
		t.Fatal("expected error without handlers")
	}
}

func TestDisruptorHandleEventsWithRollsBackAfterNilHandler(t *testing.T) {
	t.Run("producer gating", func(t *testing.T) {
		d, err := disruptor.New(
			event.FactoryFunc[longEvent](func() longEvent { return longEvent{} }),
			1,
		)
		if err != nil {
			t.Fatalf("new disruptor: %v", err)
		}

		handler := event.HandlerFunc[longEvent](func(
			request event.Request[longEvent],
		) error {
			return nil
		})
		if _, err := d.HandleEventsWith(handler, nil); err == nil {
			t.Fatal("expected nil handler error")
		}

		if _, err := d.RingBuffer().TryNext(); err != nil {
			t.Fatalf("first try next after failed registration: %v", err)
		}
		if _, err := d.RingBuffer().TryNext(); err != nil {
			t.Fatalf("second try next after failed registration: %v", err)
		}
	})

	t.Run("processor list", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		d, err := disruptor.New(
			event.FactoryFunc[longEvent](func() longEvent { return longEvent{} }),
			8,
		)
		if err != nil {
			t.Fatalf("new disruptor: %v", err)
		}

		stale := make(chan struct{}, 1)
		staleHandler := event.HandlerFunc[longEvent](func(
			request event.Request[longEvent],
		) error {
			stale <- struct{}{}
			return nil
		})
		if _, err := d.HandleEventsWith(staleHandler, nil); err == nil {
			t.Fatal("expected nil handler error")
		}

		active := make(chan struct{}, 1)
		activeHandler := event.HandlerFunc[longEvent](func(
			request event.Request[longEvent],
		) error {
			active <- struct{}{}
			return nil
		})
		if _, err := d.HandleEventsWith(activeHandler); err != nil {
			t.Fatalf("handle events after failed registration: %v", err)
		}
		if err := d.Start(ctx); err != nil {
			t.Fatalf("start disruptor: %v", err)
		}
		defer d.Stop()

		publishValues(t, d.RingBuffer(), 1)
		waitForSignal(t, active)
		select {
		case <-stale:
			t.Fatal("stale processor handled event after failed registration")
		case <-time.After(50 * time.Millisecond):
		}
	})
}

func TestDisruptorRejectsHandlerRegistrationAfterStart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d, err := disruptor.New(
		event.FactoryFunc[longEvent](func() longEvent { return longEvent{} }),
		8,
	)
	if err != nil {
		t.Fatalf("new disruptor: %v", err)
	}

	if err := d.Start(ctx); err != nil {
		t.Fatalf("start disruptor: %v", err)
	}
	defer d.Stop()

	_, err = d.HandleEventsWith(event.HandlerFunc[longEvent](
		func(request event.Request[longEvent]) error {
			return nil
		},
	))
	if !errors.Is(err, disruptor.ErrClosed) {
		t.Fatalf("handle events with error = %v, want ErrClosed", err)
	}
}

func TestDisruptorWaitReturnsProcessorError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d, err := disruptor.New(
		event.FactoryFunc[longEvent](func() longEvent { return longEvent{} }),
		8,
	)
	if err != nil {
		t.Fatalf("new disruptor: %v", err)
	}

	handlerErr := errDisruptorTestHandler
	_, err = d.HandleEventsWith(event.HandlerFunc[longEvent](func(request event.Request[longEvent]) error {
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
		event.FactoryFunc[longEvent](func() longEvent { return longEvent{} }),
		8,
	)
	if err != nil {
		t.Fatalf("new disruptor: %v", err)
	}

	handlerErr := errDisruptorTestHandler
	_, err = d.HandleEventsWith(
		event.HandlerFunc[longEvent](func(request event.Request[longEvent]) error {
			return handlerErr
		}),
		event.HandlerFunc[longEvent](func(request event.Request[longEvent]) error {
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
	task := disruptorWaitTask{
		disruptor: d,
		done:      waitDone,
	}
	go task.run()

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

type disruptorWaitTask struct {
	disruptor *disruptor.Disruptor[longEvent]
	done      chan<- error
}

func (task disruptorWaitTask) run() {
	task.done <- task.disruptor.Wait()
}

var errDisruptorTestHandler = &testHandlerError{}

type testHandlerError struct{}

func (e *testHandlerError) Error() string {
	return "handler failed"
}
