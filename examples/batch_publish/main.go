package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/photowey/disruptor.go/pkg/disruptor"
)

type batchEvent struct {
	Value int64
}

type batchEventFactory struct{}

func (batchEventFactory) NewEvent() batchEvent {
	return batchEvent{}
}

type batchHandler struct {
	values chan<- int64
}

func (h batchHandler) OnEvent(request disruptor.EventRequest[batchEvent]) error {
	h.values <- request.Event.Value
	return nil
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d, err := disruptor.New(batchEventFactory{}, 1024)
	if err != nil {
		panic(err)
	}

	values := make(chan int64, 4)
	if _, err := d.HandleEventsWith(batchHandler{values: values}); err != nil {
		panic(err)
	}
	if err := d.Start(ctx); err != nil {
		panic(err)
	}

	highest, err := d.RingBuffer().NextN(ctx, 4)
	if err != nil {
		panic(err)
	}
	lowest := highest - 3
	for sequence := lowest; sequence <= highest; sequence++ {
		event := d.RingBuffer().Get(sequence)
		event.Value = sequence - lowest + 1
	}
	d.RingBuffer().PublishRange(lowest, highest)

	var sum int64
	parts := make([]string, 0, 4)
	for range 4 {
		value := <-values
		sum += value
		parts = append(parts, fmt.Sprintf("%d", value))
	}

	fmt.Printf("batch=%s sum=%d\n", strings.Join(parts, ","), sum)

	d.Stop()
	if err := d.Wait(); err != nil {
		panic(err)
	}
}
