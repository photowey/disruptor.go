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

package ringbuffer_test

import (
	"context"
	"fmt"

	"github.com/photowey/disruptor.go/pkg/event"
	"github.com/photowey/disruptor.go/pkg/ringbuffer"
)

type ringEvent struct {
	Value int64
}

type ringFactory struct{}

func (ringFactory) NewEvent() ringEvent {
	return ringEvent{}
}

type ringTranslator struct {
	Value int64
}

func (t ringTranslator) Translate(request event.TranslateRequest[ringEvent]) {
	request.Event.Value = t.Value
}

func ExampleRingBuffer_PublishEvent() {
	ctx := context.Background()
	rb, err := ringbuffer.New(
		ringFactory{},
		8,
		ringbuffer.WithProducerType(ringbuffer.ProducerTypeSingle),
	)
	if err != nil {
		panic(err)
	}

	if err := rb.PublishEvent(ctx, ringTranslator{Value: 42}); err != nil {
		panic(err)
	}

	fmt.Println(rb.Get(0).Value)

	// Output:
	// 42
}
