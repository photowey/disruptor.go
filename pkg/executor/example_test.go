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
	"time"

	"github.com/photowey/disruptor.go/pkg/executor"
)

type exampleOrder struct {
	id    int64
	items []string
}

type exampleOrderStep struct {
	name  string
	value int64
}

type exampleValidateTask struct {
	order exampleOrder
}

func (t exampleValidateTask) Execute(context.Context) (exampleOrderStep, error) {
	if len(t.order.items) == 0 {
		return exampleOrderStep{}, fmt.Errorf("order %d has no items", t.order.id)
	}

	return exampleOrderStep{name: "validate", value: 1}, nil
}

type examplePriceTask struct {
	order exampleOrder
}

func (t examplePriceTask) Execute(context.Context) (exampleOrderStep, error) {
	return exampleOrderStep{
		name:  "price",
		value: int64(len(t.order.items)) * 599,
	}, nil
}

type exampleReceiptTask struct {
	orderID int64
}

func (t exampleReceiptTask) Apply(
	_ context.Context,
	steps []exampleOrderStep,
) (string, error) {
	var total int64
	for _, step := range steps {
		if step.name == "price" {
			total = step.value
		}
	}

	return fmt.Sprintf("order-%d:%d", t.orderID, total), nil
}

type exampleExternalProducer struct {
	value string
}

func (p exampleExternalProducer) Complete(
	promise executor.Promise[string],
) {
	promise.Complete(p.value)
}

func ExampleSubmit_orderWorkflow() {
	ctx := context.Background()
	pool, err := executor.NewFixedWorkerExecutor(
		executor.WithWorkers(2),
		executor.WithQueueSize(2),
		executor.WithRejectPolicy(executor.RejectPolicyReject),
	)
	if err != nil {
		panic(err)
	}
	defer shutdownExampleExecutor(pool)

	order := exampleOrder{
		id:    1001,
		items: []string{"book", "pen"},
	}
	validateFuture, err := executor.Submit(
		ctx,
		pool,
		exampleValidateTask{order: order},
		executor.WithTaskName("validate"),
	)
	if err != nil {
		panic(err)
	}
	priceFuture, err := executor.Submit(
		ctx,
		pool,
		examplePriceTask{order: order},
		executor.WithTaskName("price"),
	)
	if err != nil {
		panic(err)
	}

	stepsFuture := executor.All(validateFuture, priceFuture)
	receiptFuture, err := executor.ThenApply(
		ctx,
		pool,
		stepsFuture,
		exampleReceiptTask{orderID: order.id},
	)
	if err != nil {
		panic(err)
	}
	receipt, err := receiptFuture.Await(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println(receipt)

	// Output: order-1001:1198
}

func ExamplePromise_externalCompletion() {
	ctx := context.Background()
	promise := executor.NewPromise[string]()
	future := promise.Future()

	producer := exampleExternalProducer{value: "indexed"}
	go producer.Complete(promise)

	value, err := future.Await(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println(value)

	// Output: indexed
}

func shutdownExampleExecutor(pool executor.Executor) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := pool.Shutdown(ctx); err != nil {
		panic(err)
	}
}
