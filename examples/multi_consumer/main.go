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

	"github.com/photowey/disruptor.go/pkg/disruptor"
)

type orderEvent struct {
	ID int64
}

type orderEventFactory struct{}

func (orderEventFactory) NewEvent() orderEvent {
	return orderEvent{}
}

type orderHandler struct {
	name    string
	results chan<- string
}

func (h orderHandler) OnEvent(request disruptor.EventRequest[orderEvent]) error {
	h.results <- fmt.Sprintf("%s:%d", h.name, request.Event.ID)
	return nil
}

type orderTranslator struct {
	id int64
}

func (t orderTranslator) Translate(request disruptor.TranslateRequest[orderEvent]) {
	request.Event.ID = t.id
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d, err := disruptor.New(
		orderEventFactory{},
		1024,
	)
	if err != nil {
		panic(err)
	}

	results := make(chan string, 2)
	if _, err := d.HandleEventsWith(
		orderHandler{name: "audit", results: results},
		orderHandler{name: "projection", results: results},
	); err != nil {
		panic(err)
	}
	if err := d.Start(ctx); err != nil {
		panic(err)
	}

	err = d.RingBuffer().PublishEvent(ctx, orderTranslator{id: 1001})
	if err != nil {
		panic(err)
	}

	values := []string{<-results, <-results}
	sort.Strings(values)
	fmt.Println(strings.Join(values, ","))

	d.Stop()
	if err := d.Wait(); err != nil {
		panic(err)
	}
}
