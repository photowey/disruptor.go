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

type metricEventFactory struct{}

func (metricEventFactory) NewEvent() metricEvent {
	return metricEvent{}
}

type counterMetricsSink struct {
	published *atomic.Int64
	handled   *atomic.Int64
}

func (s counterMetricsSink) OnPublish(request disruptor.PublishMetric) {
	s.published.Add(request.BatchSize)
}

func (s counterMetricsSink) OnBatchStart(request disruptor.BatchMetric) {}

func (s counterMetricsSink) OnEventHandled(request disruptor.EventMetric) {
	s.handled.Add(1)
}

func (s counterMetricsSink) OnWait(request disruptor.WaitMetric) {}

func (s counterMetricsSink) OnProcessorState(request disruptor.ProcessorMetric) {}

type signalHandler struct {
	done chan<- struct{}
}

func (h signalHandler) OnEvent(request disruptor.EventRequest[metricEvent]) error {
	h.done <- struct{}{}
	return nil
}

type metricTranslator struct {
	value int64
}

func (t metricTranslator) Translate(request disruptor.TranslateRequest[metricEvent]) {
	request.Event.Value = t.value
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var published atomic.Int64
	var handled atomic.Int64
	metrics := counterMetricsSink{
		published: &published,
		handled:   &handled,
	}

	d, err := disruptor.New(
		metricEventFactory{},
		1024,
		disruptor.WithMetricsSink(metrics),
	)
	if err != nil {
		panic(err)
	}

	done := make(chan struct{}, 1)
	_, err = d.HandleEventsWith(signalHandler{done: done})
	if err != nil {
		panic(err)
	}
	if err := d.Start(ctx); err != nil {
		panic(err)
	}

	err = d.RingBuffer().PublishEvent(ctx, metricTranslator{value: 7})
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
