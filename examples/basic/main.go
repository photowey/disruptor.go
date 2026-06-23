package main

import (
	"context"
	"fmt"

	"github.com/photowey/disruptor.go/pkg/disruptor"
)

type longEvent struct {
	Value int64
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d, err := disruptor.New(
		disruptor.EventFactoryFunc[longEvent](func() longEvent { return longEvent{} }),
		1024,
	)
	if err != nil {
		panic(err)
	}

	done := make(chan int64, 1)
	_, err = d.HandleEventsWith(disruptor.EventHandlerFunc[longEvent](func(
		request disruptor.EventRequest[longEvent],
	) error {
		done <- request.Event.Value
		return nil
	}))
	if err != nil {
		panic(err)
	}
	if err := d.Start(ctx); err != nil {
		panic(err)
	}

	err = d.RingBuffer().PublishEvent(ctx, disruptor.EventTranslatorFunc[longEvent](func(
		request disruptor.TranslateRequest[longEvent],
	) {
		request.Event.Value = 42
	}))
	if err != nil {
		panic(err)
	}

	fmt.Println(<-done)

	d.Stop()
	if err := d.Wait(); err != nil {
		panic(err)
	}
}
