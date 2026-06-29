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

package benchmarks

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/photowey/disruptor.go/pkg/disruptor"
	"github.com/photowey/disruptor.go/pkg/event"
	"github.com/photowey/disruptor.go/pkg/ringbuffer"
	"github.com/photowey/disruptor.go/pkg/wait"
)

type benchEvent struct {
	Value int64
}

var benchmarkValueSink atomic.Int64

func BenchmarkE2EDisruptor(b *testing.B) {
	for _, waitStrategy := range benchmarkWaitStrategyCases() {
		for _, consumerCount := range []int{1, 4} {
			name := fmt.Sprintf("%s_1p_%dc", waitStrategy.name, consumerCount)
			b.Run(name, func(b *testing.B) {
				benchmarkDisruptorE2E(b, waitStrategy.factory(), consumerCount)
			})
		}
	}
}

func BenchmarkRingBufferMatrix(b *testing.B) {
	for _, ringSize := range benchmarkRingSizes() {
		b.Run(fmt.Sprintf("ring_%d", ringSize), func(b *testing.B) {
			for _, payloadShape := range benchmarkPayloadShapes() {
				b.Run(payloadShape, func(b *testing.B) {
					switch payloadShape {
					case "small_value":
						benchmarkRingBufferMatrixSmallValue(b, ringSize)
					case "padded_value":
						benchmarkRingBufferMatrixPaddedValue(b, ringSize)
					case "pointer_adapter":
						benchmarkRingBufferMatrixPointerAdapter(b, ringSize)
					default:
						b.Fatalf("unknown payload shape: %s", payloadShape)
					}
				})
			}
		})
	}
}

func BenchmarkE2EDisruptorParallelProducers(b *testing.B) {
	for _, waitStrategy := range benchmarkWaitStrategyCases() {
		for _, consumerCount := range []int{1, 4} {
			name := fmt.Sprintf("%s_mp_%dc", waitStrategy.name, consumerCount)
			b.Run(name, func(b *testing.B) {
				benchmarkDisruptorParallelProducers(b, waitStrategy.factory(), consumerCount)
			})
		}
	}
}

func benchmarkRingBufferMatrixSmallValue(b *testing.B, ringSize int) {
	b.Helper()

	rb, err := ringbuffer.New(
		event.FactoryFunc[benchEvent](func() benchEvent { return benchEvent{} }),
		ringSize,
	)
	if err != nil {
		b.Fatalf("new ring buffer: %v", err)
	}

	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		sequence, err := rb.Next(ctx)
		if err != nil {
			b.Fatalf("next: %v", err)
		}
		rb.Get(sequence).Value = sequence
		rb.Publish(sequence)
	}
}

func benchmarkRingBufferMatrixPaddedValue(b *testing.B, ringSize int) {
	b.Helper()

	rb, err := ringbuffer.New(
		event.FactoryFunc[paddedBenchEvent](func() paddedBenchEvent {
			return paddedBenchEvent{}
		}),
		ringSize,
	)
	if err != nil {
		b.Fatalf("new ring buffer: %v", err)
	}

	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		sequence, err := rb.Next(ctx)
		if err != nil {
			b.Fatalf("next: %v", err)
		}
		rb.Get(sequence).Value = sequence
		rb.Publish(sequence)
	}
}

func benchmarkRingBufferMatrixPointerAdapter(b *testing.B, ringSize int) {
	b.Helper()

	rb, err := ringbuffer.New(
		event.FactoryFunc[*benchEvent](func() *benchEvent { return &benchEvent{} }),
		ringSize,
	)
	if err != nil {
		b.Fatalf("new ring buffer: %v", err)
	}

	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		sequence, err := rb.Next(ctx)
		if err != nil {
			b.Fatalf("next: %v", err)
		}
		(*rb.Get(sequence)).Value = sequence
		rb.Publish(sequence)
	}
}

func benchmarkDisruptorE2E(
	b *testing.B,
	waitStrategy wait.Strategy,
	consumerCount int,
) {
	b.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d, err := disruptor.New(
		event.FactoryFunc[benchEvent](func() benchEvent { return benchEvent{} }),
		65536,
		ringbuffer.WithWaitStrategy(waitStrategy),
	)
	if err != nil {
		b.Fatalf("new disruptor: %v", err)
	}

	var processed atomic.Int64
	handlers := make([]event.Handler[benchEvent], 0, consumerCount)
	for range consumerCount {
		handlers = append(handlers, event.HandlerFunc[benchEvent](func(
			request event.Request[benchEvent],
		) error {
			benchmarkValueSink.Store(request.Event.Value)
			processed.Add(1)
			return nil
		}))
	}
	if _, err := d.HandleEventsWith(handlers...); err != nil {
		b.Fatalf("handle events with: %v", err)
	}
	if err := d.Start(ctx); err != nil {
		b.Fatalf("start disruptor: %v", err)
	}

	publishContext := context.Background()
	var published int64
	started := time.Now()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		sequence, err := d.RingBuffer().Next(publishContext)
		if err != nil {
			b.Fatalf("next: %v", err)
		}
		d.RingBuffer().Get(sequence).Value = published
		d.RingBuffer().Publish(sequence)
		published++
	}
	b.StopTimer()

	target := published * int64(consumerCount)
	waitForBenchmarkEvents(b, &processed, target)

	d.Stop()
	if err := d.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		b.Fatalf("wait disruptor: %v", err)
	}

	elapsed := time.Since(started).Seconds()
	if elapsed > 0 {
		b.ReportMetric(float64(published)/elapsed, "events/s")
	}
}

func benchmarkDisruptorParallelProducers(
	b *testing.B,
	waitStrategy wait.Strategy,
	consumerCount int,
) {
	b.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d, err := disruptor.New(
		event.FactoryFunc[benchEvent](func() benchEvent { return benchEvent{} }),
		65536,
		ringbuffer.WithProducerType(ringbuffer.ProducerTypeMulti),
		ringbuffer.WithWaitStrategy(waitStrategy),
	)
	if err != nil {
		b.Fatalf("new disruptor: %v", err)
	}

	var processed atomic.Int64
	handlers := make([]event.Handler[benchEvent], 0, consumerCount)
	for range consumerCount {
		handlers = append(handlers, event.HandlerFunc[benchEvent](func(
			request event.Request[benchEvent],
		) error {
			benchmarkValueSink.Store(request.Event.Value)
			processed.Add(1)
			return nil
		}))
	}
	if _, err := d.HandleEventsWith(handlers...); err != nil {
		b.Fatalf("handle events with: %v", err)
	}
	if err := d.Start(ctx); err != nil {
		b.Fatalf("start disruptor: %v", err)
	}

	publishContext := context.Background()
	var published atomic.Int64
	started := time.Now()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sequence, err := d.RingBuffer().Next(publishContext)
			if err != nil {
				b.Fatalf("next: %v", err)
			}
			value := published.Add(1)
			d.RingBuffer().Get(sequence).Value = value
			d.RingBuffer().Publish(sequence)
		}
	})
	b.StopTimer()

	publishedCount := published.Load()
	target := publishedCount * int64(consumerCount)
	waitForBenchmarkEvents(b, &processed, target)

	d.Stop()
	if err := d.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		b.Fatalf("wait disruptor: %v", err)
	}

	elapsed := time.Since(started).Seconds()
	if elapsed > 0 {
		b.ReportMetric(float64(publishedCount)/elapsed, "events/s")
	}
}

func BenchmarkRingBufferParallelProducers(b *testing.B) {
	rb, err := ringbuffer.New(
		event.FactoryFunc[benchEvent](func() benchEvent { return benchEvent{} }),
		65536,
		ringbuffer.WithProducerType(ringbuffer.ProducerTypeMulti),
	)
	if err != nil {
		b.Fatalf("new ring buffer: %v", err)
	}

	ctx := context.Background()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sequence, err := rb.Next(ctx)
			if err != nil {
				b.Fatalf("next: %v", err)
			}
			rb.Get(sequence).Value = sequence
			rb.Publish(sequence)
		}
	})
}

func BenchmarkChannelComparison(b *testing.B) {
	for _, tt := range []struct {
		name     string
		capacity int
	}{
		{name: "unbuffered_value", capacity: 0},
		{name: "buffered_value_65536", capacity: 65536},
	} {
		b.Run(tt.name, func(b *testing.B) {
			benchmarkChannelValue(b, tt.capacity)
		})
	}

	b.Run("buffered_pointer_allocating_65536", func(b *testing.B) {
		benchmarkChannelPointer(b, 65536)
	})
	b.Run("buffered_value_non_blocking_spin_1024", func(b *testing.B) {
		benchmarkChannelNonBlockingSpin(b, 1024)
	})
	b.Run("sync_cond_ring_65536", func(b *testing.B) {
		benchmarkCondQueueValue(b, 65536)
	})
}

func benchmarkChannelValue(b *testing.B, capacity int) {
	b.Helper()

	ch := make(chan benchEvent, capacity)
	done := make(chan int64, 1)
	task := channelValueConsumerTask{
		events: ch,
		done:   done,
	}
	go task.run()

	var value int64
	b.ReportAllocs()
	for b.Loop() {
		ch <- benchEvent{Value: value}
		value++
	}

	close(ch)
	benchmarkValueSink.Store(<-done)
}

func benchmarkChannelPointer(b *testing.B, capacity int) {
	b.Helper()

	ch := make(chan *benchEvent, capacity)
	done := make(chan int64, 1)
	task := channelPointerConsumerTask{
		events: ch,
		done:   done,
	}
	go task.run()

	var value int64
	b.ReportAllocs()
	for b.Loop() {
		ch <- &benchEvent{Value: value}
		value++
	}

	close(ch)
	benchmarkValueSink.Store(<-done)
}

func benchmarkChannelNonBlockingSpin(b *testing.B, capacity int) {
	b.Helper()

	ch := make(chan benchEvent, capacity)
	done := make(chan struct{})
	task := channelNonBlockingSpinTask{
		events: ch,
		done:   done,
	}
	go task.run()

	var dropped int64
	var value int64
	b.ReportAllocs()
	for b.Loop() {
		select {
		case ch <- benchEvent{Value: value}:
		default:
			dropped++
		}
		value++
	}

	close(done)
	benchmarkValueSink.Store(dropped)
}

func benchmarkCondQueueValue(b *testing.B, capacity int) {
	b.Helper()

	queue := newBenchmarkCondQueue(capacity)
	done := make(chan int64, 1)
	task := condQueueValueConsumerTask{
		queue: queue,
		done:  done,
	}
	go task.run()

	var value int64
	b.ReportAllocs()
	for b.Loop() {
		queue.push(benchEvent{Value: value})
		value++
	}

	queue.close()
	benchmarkValueSink.Store(<-done)
}

type channelValueConsumerTask struct {
	events <-chan benchEvent
	done   chan<- int64
}

func (task channelValueConsumerTask) run() {
	var sum int64
	for event := range task.events {
		sum += event.Value
	}
	task.done <- sum
}

type channelPointerConsumerTask struct {
	events <-chan *benchEvent
	done   chan<- int64
}

func (task channelPointerConsumerTask) run() {
	var sum int64
	for event := range task.events {
		sum += event.Value
	}
	task.done <- sum
}

type channelNonBlockingSpinTask struct {
	events <-chan benchEvent
	done   <-chan struct{}
}

func (task channelNonBlockingSpinTask) run() {
	for {
		select {
		case <-task.done:
			return
		case event := <-task.events:
			benchmarkValueSink.Store(event.Value)
		default:
			runtime.Gosched()
		}
	}
}

type condQueueValueConsumerTask struct {
	queue *benchmarkCondQueue
	done  chan<- int64
}

func (task condQueueValueConsumerTask) run() {
	var sum int64
	for {
		event, ok := task.queue.pop()
		if !ok {
			break
		}
		sum += event.Value
	}
	task.done <- sum
}

type benchmarkCondQueue struct {
	mu       sync.Mutex
	notEmpty *sync.Cond
	notFull  *sync.Cond

	buffer []benchEvent
	head   int
	tail   int
	count  int
	closed bool
}

func newBenchmarkCondQueue(capacity int) *benchmarkCondQueue {
	queue := &benchmarkCondQueue{
		buffer: make([]benchEvent, capacity),
	}
	queue.notEmpty = sync.NewCond(&queue.mu)
	queue.notFull = sync.NewCond(&queue.mu)

	return queue
}

func (q *benchmarkCondQueue) push(event benchEvent) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for q.count == len(q.buffer) && !q.closed {
		q.notFull.Wait()
	}
	if q.closed {
		return
	}

	q.buffer[q.tail] = event
	q.tail = (q.tail + 1) % len(q.buffer)
	q.count++
	q.notEmpty.Signal()
}

func (q *benchmarkCondQueue) pop() (benchEvent, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for q.count == 0 && !q.closed {
		q.notEmpty.Wait()
	}
	if q.count == 0 && q.closed {
		return benchEvent{}, false
	}

	event := q.buffer[q.head]
	q.head = (q.head + 1) % len(q.buffer)
	q.count--
	q.notFull.Signal()

	return event, true
}

func (q *benchmarkCondQueue) close() {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.closed = true
	q.notEmpty.Broadcast()
	q.notFull.Broadcast()
}

func waitForBenchmarkEvents(
	b *testing.B,
	processed *atomic.Int64,
	target int64,
) {
	b.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for processed.Load() < target {
		if time.Now().After(deadline) {
			b.Fatalf(
				"timed out waiting for processed events: got %d, want %d",
				processed.Load(),
				target,
			)
		}

		runtime.Gosched()
	}
}

func BenchmarkRingBufferBatchSizes(b *testing.B) {
	for _, batchSize := range benchmarkBatchSizes() {
		b.Run(fmt.Sprintf("batch_%d", batchSize), func(b *testing.B) {
			rb, err := ringbuffer.New(
				event.FactoryFunc[benchEvent](func() benchEvent { return benchEvent{} }),
				65536,
			)
			if err != nil {
				b.Fatalf("new ring buffer: %v", err)
			}

			ctx := context.Background()
			b.ReportAllocs()
			for b.Loop() {
				hi, err := rb.NextN(ctx, batchSize)
				if err != nil {
					b.Fatalf("next batch: %v", err)
				}
				lo := hi - batchSize + 1
				for sequence := lo; sequence <= hi; sequence++ {
					rb.Get(sequence).Value = sequence
				}
				rb.PublishRange(lo, hi)
			}
		})
	}
}
