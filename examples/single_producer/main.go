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
