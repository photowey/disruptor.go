package main

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/photowey/disruptor.go/pkg/disruptor"
)

type recoveryEvent struct {
	Value int64
}

type recoveryEventFactory struct{}

func (recoveryEventFactory) NewEvent() recoveryEvent {
	return recoveryEvent{}
}

type retryingHandler struct {
	attempts *atomic.Int64
	done     chan<- int64
}

func (h retryingHandler) OnEvent(request disruptor.EventRequest[recoveryEvent]) error {
	attempt := h.attempts.Add(1)
	if attempt <= 2 {
		return errors.New("temporary failure")
	}

	h.done <- request.Event.Value
	return nil
}

type recoveryTranslator struct {
	value int64
}

func (t recoveryTranslator) Translate(request disruptor.TranslateRequest[recoveryEvent]) {
	request.Event.Value = t.value
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d, err := disruptor.New(
		recoveryEventFactory{},
		1024,
	)
	if err != nil {
		panic(err)
	}

	var attempts atomic.Int64
	done := make(chan int64, 1)
	handler := retryingHandler{attempts: &attempts, done: done}

	retryHandler, err := disruptor.NewRetryExceptionHandler[recoveryEvent](
		2,
		disruptor.ExceptionActionHalt,
	)
	if err != nil {
		panic(err)
	}
	_, err = d.HandleEventsWithOptions(
		[]disruptor.EventHandler[recoveryEvent]{handler},
		disruptor.WithExceptionHandler[recoveryEvent](retryHandler),
	)
	if err != nil {
		panic(err)
	}
	if err := d.Start(ctx); err != nil {
		panic(err)
	}

	err = d.RingBuffer().PublishEvent(ctx, recoveryTranslator{value: 9})
	if err != nil {
		panic(err)
	}

	fmt.Printf("value=%d attempts=%d\n", <-done, attempts.Load())

	d.Stop()
	if err := d.Wait(); err != nil {
		panic(err)
	}
}
