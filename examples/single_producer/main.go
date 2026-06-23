package main

import (
	"context"
	"fmt"

	"github.com/photowey/disruptor.go/pkg/disruptor"
)

type singleEvent struct {
	Value int64
}

type singleEventFactory struct{}

func (singleEventFactory) NewEvent() singleEvent {
	return singleEvent{}
}

type singleHandler struct {
	done chan<- int64
}

func (h singleHandler) OnEvent(request disruptor.EventRequest[singleEvent]) error {
	h.done <- request.Event.Value
	return nil
}

type singleTranslator struct {
	value int64
}

func (t singleTranslator) Translate(request disruptor.TranslateRequest[singleEvent]) {
	request.Event.Value = t.value
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d, err := disruptor.New(
		singleEventFactory{},
		1024,
		disruptor.WithProducerType(disruptor.ProducerTypeSingle),
	)
	if err != nil {
		panic(err)
	}

	done := make(chan int64, 1)
	if _, err := d.HandleEventsWith(singleHandler{done: done}); err != nil {
		panic(err)
	}
	if err := d.Start(ctx); err != nil {
		panic(err)
	}

	err = d.RingBuffer().PublishEvent(ctx, singleTranslator{value: 7})
	if err != nil {
		panic(err)
	}

	fmt.Printf("single=%d\n", <-done)

	d.Stop()
	if err := d.Wait(); err != nil {
		panic(err)
	}
}
