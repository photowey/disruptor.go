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

package event_test

import (
	"context"
	"fmt"

	"github.com/photowey/disruptor.go/pkg/event"
)

type exampleEvent struct {
	Value int64
}

type exampleTranslator struct {
	Value int64
}

func (t exampleTranslator) Translate(request event.TranslateRequest[exampleEvent]) {
	request.Event.Value = t.Value
}

type exampleHandler struct{}

func (h exampleHandler) OnEvent(request event.Request[exampleEvent]) error {
	fmt.Printf("sequence=%d value=%d\n", request.Sequence, request.Event.Value)

	return nil
}

func Example() {
	item := exampleEvent{}
	translator := exampleTranslator{Value: 42}
	translator.Translate(event.TranslateRequest[exampleEvent]{
		Context:  context.Background(),
		Event:    &item,
		Sequence: 7,
	})

	handler := exampleHandler{}
	if err := handler.OnEvent(event.Request[exampleEvent]{
		Context:  context.Background(),
		Event:    &item,
		Sequence: 7,
	}); err != nil {
		panic(err)
	}

	// Output:
	// sequence=7 value=42
}
