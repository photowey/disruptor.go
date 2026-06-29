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
	"testing"
	"time"

	"github.com/photowey/disruptor.go/pkg/event"
	"github.com/photowey/disruptor.go/pkg/ringbuffer"
)

type longEvent struct {
	Value int64
}

func waitForSignal(t *testing.T, done <-chan struct{}) {
	t.Helper()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for signal")
	}
}

func publishValues(t *testing.T, rb *ringbuffer.RingBuffer[longEvent], values ...int64) {
	t.Helper()

	ctx := context.Background()
	for _, value := range values {
		err := rb.PublishEvent(ctx, event.TranslatorFunc[longEvent](func(request event.TranslateRequest[longEvent]) {
			request.Event.Value = value
		}))
		if err != nil {
			t.Fatalf("publish event: %v", err)
		}
	}
}
