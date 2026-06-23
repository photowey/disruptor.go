package main

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/photowey/disruptor.go/pkg/disruptor"
)

type orderEvent struct {
	ID int64
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d, err := disruptor.New(
		disruptor.EventFactoryFunc[orderEvent](func() orderEvent { return orderEvent{} }),
		1024,
	)
	if err != nil {
		panic(err)
	}

	results := make(chan string, 2)
	handler := func(name string) disruptor.EventHandler[orderEvent] {
		return disruptor.EventHandlerFunc[orderEvent](func(
			request disruptor.EventRequest[orderEvent],
		) error {
			results <- fmt.Sprintf("%s:%d", name, request.Event.ID)
			return nil
		})
	}

	if _, err := d.HandleEventsWith(handler("audit"), handler("projection")); err != nil {
		panic(err)
	}
	if err := d.Start(ctx); err != nil {
		panic(err)
	}

	err = d.RingBuffer().PublishEvent(ctx, disruptor.EventTranslatorFunc[orderEvent](func(
		request disruptor.TranslateRequest[orderEvent],
	) {
		request.Event.ID = 1001
	}))
	if err != nil {
		panic(err)
	}

	values := []string{<-results, <-results}
	sort.Strings(values)
	fmt.Println(strings.Join(values, ","))

	d.Stop()
	if err := d.Wait(); err != nil {
		panic(err)
	}
}
