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

type quickEvent struct {
	Value int64
}

type quickEventFactory struct{}

func (quickEventFactory) NewEvent() quickEvent {
	return quickEvent{}
}

type quickTranslator struct {
	value int64
}

func (t quickTranslator) Translate(request disruptor.TranslateRequest[quickEvent]) {
	request.Event.Value = t.value
}

type quickGraphHandler struct {
	steps chan<- string
}

func (h quickGraphHandler) OnEvent(request event.Request[quickEvent]) error {
	h.steps <- fmt.Sprintf("%s:%d", request.Node.NodeName, request.Event.Value)
	return nil
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d, err := disruptor.New(quickEventFactory{}, 1024)
	if err != nil {
		panic(err)
	}

	steps := make(chan string, 2)
	graph := topology.Must[quickEvent]("quickstart").
		MustNode("validate", quickGraphHandler{steps: steps}).
		MustNode("persist", quickGraphHandler{steps: steps}).
		MustEdge(topology.StartNode, "validate").
		MustEdge("validate", "persist").
		MustEdge("persist", topology.EndNode)

	if _, err := d.HandleGraph(graph); err != nil {
		panic(err)
	}
	if err := d.Start(ctx); err != nil {
		panic(err)
	}

	if err := d.RingBuffer().PublishEvent(ctx, quickTranslator{value: 42}); err != nil {
		panic(err)
	}

	handled := []string{<-steps, <-steps}
	fmt.Println(strings.Join(handled, ","))

	d.Stop()
	if err := d.Wait(); err != nil {
		panic(err)
	}
}
