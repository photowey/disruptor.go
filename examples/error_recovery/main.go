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
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/photowey/disruptor.go/pkg/disruptor"
	"github.com/photowey/disruptor.go/pkg/event"
	"github.com/photowey/disruptor.go/pkg/processor"
)

type recoveryEvent struct {
	Value int64
}

type recoveryEventFactory struct{}

func (recoveryEventFactory) NewEvent() recoveryEvent {
	return recoveryEvent{}
}

type retryingHandler struct {
	attempts *atomic.Int64
	done     chan<- int64
}

func (h retryingHandler) OnEvent(request event.Request[recoveryEvent]) error {
	attempt := h.attempts.Add(1)
	if attempt <= 2 {
		return errors.New("temporary failure")
	}

	h.done <- request.Event.Value
	return nil
}

type recoveryTranslator struct {
	value int64
}

func (t recoveryTranslator) Translate(request event.TranslateRequest[recoveryEvent]) {
	request.Event.Value = t.value
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d, err := disruptor.New(
		recoveryEventFactory{},
		1024,
	)
	if err != nil {
		panic(err)
	}

	var attempts atomic.Int64
	done := make(chan int64, 1)
	handler := retryingHandler{attempts: &attempts, done: done}

	retryHandler, err := event.NewRetryExceptionHandler[recoveryEvent](
		2,
		event.ExceptionActionHalt,
	)
	if err != nil {
		panic(err)
	}
	_, err = d.HandleEventsWithOptions(
		[]event.Handler[recoveryEvent]{handler},
		processor.WithExceptionHandler[recoveryEvent](retryHandler),
	)
	if err != nil {
		panic(err)
	}
	if err := d.Start(ctx); err != nil {
		panic(err)
	}

	err = d.RingBuffer().PublishEvent(ctx, recoveryTranslator{value: 9})
	if err != nil {
		panic(err)
	}

	fmt.Printf("value=%d attempts=%d\n", <-done, attempts.Load())

	d.Stop()
	if err := d.Wait(); err != nil {
		panic(err)
	}
}
