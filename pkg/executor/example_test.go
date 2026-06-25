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

package executor_test

import (
	"context"
	"fmt"

	"github.com/photowey/disruptor.go/pkg/executor"
)

type exampleTask struct {
	value int
}

func (t exampleTask) Execute(context.Context) (int, error) {
	return t.value * 2, nil
}

type exampleApplyTask struct{}

func (exampleApplyTask) Apply(_ context.Context, value int) (string, error) {
	return fmt.Sprintf("value=%d", value), nil
}

func ExampleSubmit() {
	ctx := context.Background()
	inline := executor.NewInlineExecutor()

	future, err := executor.Submit(ctx, inline, exampleTask{value: 21})
	if err != nil {
		panic(err)
	}
	value, err := future.Await(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println(value)

	// Output: 42
}

func ExampleThenApply() {
	ctx := context.Background()
	inline := executor.NewInlineExecutor()
	base := executor.CompletedFuture(42)

	future, err := executor.ThenApply(ctx, inline, base, exampleApplyTask{})
	if err != nil {
		panic(err)
	}
	value, err := future.Await(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println(value)

	// Output: value=42
}
