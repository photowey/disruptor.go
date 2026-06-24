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

package main

import (
	"context"
	"fmt"
	"github.com/photowey/disruptor.go/pkg/event"
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

func (h batchHandler) OnEvent(request event.Request[batchEvent]) error {
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
