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

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d, err := disruptor.New(
		disruptor.EventFactoryFunc[recoveryEvent](func() recoveryEvent { return recoveryEvent{} }),
		1024,
	)
	if err != nil {
		panic(err)
	}

	var attempts atomic.Int64
	done := make(chan int64, 1)
	handler := disruptor.EventHandlerFunc[recoveryEvent](func(
		request disruptor.EventRequest[recoveryEvent],
	) error {
		attempt := attempts.Add(1)
		if attempt <= 2 {
			return errors.New("temporary failure")
		}

		done <- request.Event.Value
		return nil
	})

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

	err = d.RingBuffer().PublishEvent(ctx, disruptor.EventTranslatorFunc[recoveryEvent](func(
		request disruptor.TranslateRequest[recoveryEvent],
	) {
		request.Event.Value = 9
	}))
	if err != nil {
		panic(err)
	}

	fmt.Printf("value=%d attempts=%d\n", <-done, attempts.Load())

	d.Stop()
	if err := d.Wait(); err != nil {
		panic(err)
	}
}
