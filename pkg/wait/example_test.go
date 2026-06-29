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

package wait_test

import (
	"context"
	"fmt"

	"github.com/photowey/disruptor.go/pkg/sequence"
	"github.com/photowey/disruptor.go/pkg/wait"
)

func ExampleNewBusySpinStrategy() {
	strategy := wait.NewBusySpinStrategy()
	cursor := sequence.New(0)

	available, err := strategy.WaitFor(wait.Request{
		Context:           context.Background(),
		RequestedSequence: 0,
		CursorSequence:    cursor,
	})
	if err != nil {
		panic(err)
	}

	fmt.Println(available)

	// Output:
	// 0
}
