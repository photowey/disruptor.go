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

package processor_test

import (
	"context"
	"fmt"

	"github.com/photowey/disruptor.go/pkg/event"
	"github.com/photowey/disruptor.go/pkg/processor"
	"github.com/photowey/disruptor.go/pkg/ringbuffer"
)

type processorEvent struct {
	Value int64
}

type processorFactory struct{}

func (processorFactory) NewEvent() processorEvent {
	return processorEvent{}
}

type processorTranslator struct {
	Value int64
}

func (t processorTranslator) Translate(
	request event.TranslateRequest[processorEvent],
) {
	request.Event.Value = t.Value
}

type processorHandler struct {
	done chan<- int64
}

func (h processorHandler) OnEvent(
	request event.Request[processorEvent],
) error {
	h.done <- request.Event.Value

	return nil
}

func ExampleNewBatchEventProcessor() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rb, err := ringbuffer.New(processorFactory{}, 8)
	if err != nil {
		panic(err)
	}

	done := make(chan int64, 1)
	eventProcessor, err := processor.NewBatchEventProcessor(
		rb,
		rb.NewBarrier(),
		processorHandler{done: done},
	)
	if err != nil {
		panic(err)
	}
	if err := eventProcessor.Start(ctx); err != nil {
		panic(err)
	}

	if err := rb.PublishEvent(ctx, processorTranslator{Value: 42}); err != nil {
		panic(err)
	}

	fmt.Println(<-done)

	eventProcessor.Stop()
	if err := eventProcessor.Wait(); err != nil {
		panic(err)
	}

	// Output:
	// 42
}
