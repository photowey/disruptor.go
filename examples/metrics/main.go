package main

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/photowey/disruptor.go/pkg/disruptor"
)

type metricEvent struct {
	Value int64
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var published atomic.Int64
	var handled atomic.Int64
	metrics := disruptor.MetricsSinkFunc{
		Publish: func(request disruptor.PublishMetric) {
			published.Add(request.BatchSize)
		},
		EventHandled: func(request disruptor.EventMetric) {
			handled.Add(1)
		},
	}

	d, err := disruptor.New(
		disruptor.EventFactoryFunc[metricEvent](func() metricEvent { return metricEvent{} }),
		1024,
		disruptor.WithMetricsSink(metrics),
	)
	if err != nil {
		panic(err)
	}

	done := make(chan struct{}, 1)
	_, err = d.HandleEventsWith(disruptor.EventHandlerFunc[metricEvent](func(
		request disruptor.EventRequest[metricEvent],
	) error {
		done <- struct{}{}
		return nil
	}))
	if err != nil {
		panic(err)
	}
	if err := d.Start(ctx); err != nil {
		panic(err)
	}

	err = d.RingBuffer().PublishEvent(ctx, disruptor.EventTranslatorFunc[metricEvent](func(
		request disruptor.TranslateRequest[metricEvent],
	) {
		request.Event.Value = 7
	}))
	if err != nil {
		panic(err)
	}

	<-done
	d.Stop()
	if err := d.Wait(); err != nil {
		panic(err)
	}

	fmt.Printf("published=%d handled=%d\n", published.Load(), handled.Load())
}
