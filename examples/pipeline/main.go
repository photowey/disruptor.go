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
	"github.com/photowey/disruptor.go/pkg/event"
	topology "github.com/photowey/disruptor.go/pkg/graph"
	"strings"

	"github.com/photowey/disruptor.go/pkg/disruptor"
)

type orderEvent struct {
	ID int64
}

type orderEventFactory struct{}

func (orderEventFactory) NewEvent() orderEvent {
	return orderEvent{}
}

type orderTranslator struct {
	id int64
}

func (t orderTranslator) Translate(request disruptor.TranslateRequest[orderEvent]) {
	request.Event.ID = t.id
}

type pipelineHandler struct {
	steps chan<- string
}

func (h pipelineHandler) OnEvent(request event.Request[orderEvent]) error {
	h.steps <- fmt.Sprintf("%s:%d", request.Node.NodeName, request.Event.ID)
	return nil
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d, err := disruptor.New(orderEventFactory{}, 1024)
	if err != nil {
		panic(err)
	}

	steps := make(chan string, 3)
	graph := topology.Must[orderEvent]("order-pipeline").
		MustNode("validate", pipelineHandler{steps: steps}).
		MustNode("enrich", pipelineHandler{steps: steps}).
		MustNode("persist", pipelineHandler{steps: steps}).
		MustEdge(topology.StartNode, "validate").
		MustEdge("validate", "enrich").
		MustEdge("enrich", "persist").
		MustEdge("persist", topology.EndNode)

	if _, err := d.HandleGraph(graph); err != nil {
		panic(err)
	}
	if err := d.Start(ctx); err != nil {
		panic(err)
	}

	if err := d.RingBuffer().PublishEvent(ctx, orderTranslator{id: 7}); err != nil {
		panic(err)
	}

	handled := []string{<-steps, <-steps, <-steps}
	fmt.Println(strings.Join(handled, ","))

	d.Stop()
	if err := d.Wait(); err != nil {
		panic(err)
	}
}
