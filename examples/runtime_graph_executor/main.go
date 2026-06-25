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
	"sort"
	"strings"
	"time"

	"github.com/photowey/disruptor.go/pkg/disruptor"
	"github.com/photowey/disruptor.go/pkg/event"
	"github.com/photowey/disruptor.go/pkg/executor"
	topology "github.com/photowey/disruptor.go/pkg/graph"
	"github.com/photowey/disruptor.go/pkg/runtimegraph"
)

type routedOrderEvent struct {
	OrderID int64
}

type routedOrderFactory struct{}

func (routedOrderFactory) NewEvent() routedOrderEvent {
	return routedOrderEvent{}
}

type routedOrderTranslator struct {
	orderID int64
}

func (t routedOrderTranslator) Translate(
	request disruptor.TranslateRequest[routedOrderEvent],
) {
	request.Event.OrderID = t.orderID
}

type routeOrderHandler struct {
	log chan<- string
}

func (h routeOrderHandler) OnEvent(
	request event.Request[routedOrderEvent],
) error {
	if err := request.Runtime.Set("route.fraud", true); err != nil {
		return err
	}
	if err := request.Runtime.Set("route.pricing", true); err != nil {
		return err
	}

	h.log <- fmt.Sprintf("route:%d", request.Event.OrderID)
	return nil
}

type branchOrderHandler struct {
	name string
	log  chan<- string
}

func (h branchOrderHandler) OnEvent(
	request event.Request[routedOrderEvent],
) error {
	h.log <- fmt.Sprintf("branch:%s:%d", h.name, request.Event.OrderID)
	return nil
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := executor.NewFixedWorkerExecutor(
		executor.WithWorkers(2),
		executor.WithQueueSize(4),
		executor.WithRejectPolicy(executor.RejectPolicyReject),
		executor.WithName("runtime-graph-example"),
	)
	if err != nil {
		panic(err)
	}

	d, err := disruptor.New(routedOrderFactory{}, 1024)
	if err != nil {
		panic(err)
	}

	events := make(chan string, 3)
	graph := runtimegraph.MustRuntimeGraph[routedOrderEvent]("external-executor-route").
		MustNode("route", routeOrderHandler{log: events}).
		MustNode("fraud", branchOrderHandler{name: "fraud", log: events}).
		MustNode("pricing", branchOrderHandler{name: "pricing", log: events}).
		MustEdge(topology.StartNode, "route").
		MustEdge(
			"route",
			"fraud",
			runtimegraph.WhenExpression[routedOrderEvent](`${route.fraud}`),
		).
		MustEdge(
			"route",
			"pricing",
			runtimegraph.WhenExpression[routedOrderEvent](`${route.pricing}`),
		).
		MustEdge("fraud", topology.EndNode).
		MustEdge("pricing", topology.EndNode)

	if _, err := d.HandleRuntimeGraph(
		graph,
		disruptor.WithRuntimeGraphExecutor[routedOrderEvent](pool),
	); err != nil {
		panic(err)
	}
	if err := d.Start(ctx); err != nil {
		panic(err)
	}

	if err := d.RingBuffer().PublishEvent(
		ctx,
		routedOrderTranslator{orderID: 31},
	); err != nil {
		panic(err)
	}

	handled := []string{<-events, <-events, <-events}
	var route string
	branches := make([]string, 0, 2)
	for _, item := range handled {
		switch {
		case strings.HasPrefix(item, "route:"):
			route = item
		default:
			branches = append(branches, item)
		}
	}
	sort.Strings(branches)

	fmt.Println(route)
	fmt.Println(strings.Join(branches, "\n"))

	d.Stop()
	if err := d.Wait(); err != nil {
		panic(err)
	}

	shutdownExecutor(pool)
	fmt.Println("executor-shutdown:caller")
}

func shutdownExecutor(pool executor.Executor) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := pool.Shutdown(ctx); err != nil {
		panic(err)
	}
}
