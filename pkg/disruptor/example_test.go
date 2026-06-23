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

package disruptor_test

import (
	"context"
	"fmt"

	"github.com/photowey/disruptor.go/pkg/disruptor"
)

type exampleEvent struct {
	Value int64
}

type exampleEventFactory struct{}

func (exampleEventFactory) NewEvent() exampleEvent {
	return exampleEvent{}
}

type exampleEventHandler struct {
	done chan<- int64
}

func (h exampleEventHandler) OnEvent(
	request disruptor.EventRequest[exampleEvent],
) error {
	h.done <- request.Event.Value
	return nil
}

type exampleEventTranslator struct {
	value int64
}

func (t exampleEventTranslator) Translate(
	request disruptor.TranslateRequest[exampleEvent],
) {
	request.Event.Value = t.value
}

func ExampleDisruptor() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d, err := disruptor.New(exampleEventFactory{}, 1024)
	if err != nil {
		panic(err)
	}

	done := make(chan int64, 1)
	_, err = d.HandleEventsWith(exampleEventHandler{done: done})
	if err != nil {
		panic(err)
	}
	if err := d.Start(ctx); err != nil {
		panic(err)
	}

	err = d.RingBuffer().PublishEvent(
		ctx,
		exampleEventTranslator{value: 42},
	)
	if err != nil {
		panic(err)
	}

	fmt.Println(<-done)

	d.Stop()
	if err := d.Wait(); err != nil {
		panic(err)
	}

	// Output:
	// 42
}
