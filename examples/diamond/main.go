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

	"github.com/photowey/disruptor.go/pkg/disruptor"
)

type diamondEvent struct {
	ID int64
}

type diamondEventFactory struct{}

func (diamondEventFactory) NewEvent() diamondEvent {
	return diamondEvent{}
}

type diamondTranslator struct {
	id int64
}

func (t diamondTranslator) Translate(request disruptor.TranslateRequest[diamondEvent]) {
	request.Event.ID = t.id
}

type branchHandler struct{}

func (branchHandler) OnEvent(request disruptor.EventRequest[diamondEvent]) error {
	return nil
}

type finalHandler struct {
	done chan<- string
}

func (h finalHandler) OnEvent(request disruptor.EventRequest[diamondEvent]) error {
	h.done <- fmt.Sprintf("diamond:%s after B+C for %d", request.Node.NodeName, request.Event.ID)
	return nil
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d, err := disruptor.New(diamondEventFactory{}, 1024)
	if err != nil {
		panic(err)
	}

	done := make(chan string, 1)
	graph := disruptor.MustGraph[diamondEvent]("diamond").
		MustNode("A", branchHandler{}).
		MustNode("B", branchHandler{}).
		MustNode("C", branchHandler{}).
		MustNode("D", finalHandler{done: done}).
		MustEdge(disruptor.GraphStartNode, "A").
		MustEdge("A", "B").
		MustEdge("A", "C")
	graph.Join("B", "C").MustTo("D")
	graph.MustEdge("D", disruptor.GraphEndNode)

	if _, err := d.HandleGraph(graph); err != nil {
		panic(err)
	}
	if err := d.Start(ctx); err != nil {
		panic(err)
	}

	if err := d.RingBuffer().PublishEvent(ctx, diamondTranslator{id: 9}); err != nil {
		panic(err)
	}

	fmt.Println(<-done)

	d.Stop()
	if err := d.Wait(); err != nil {
		panic(err)
	}
}
