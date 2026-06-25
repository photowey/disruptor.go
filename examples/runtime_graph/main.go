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
	"strings"

	"github.com/photowey/disruptor.go/pkg/disruptor"
	"github.com/photowey/disruptor.go/pkg/event"
	topology "github.com/photowey/disruptor.go/pkg/graph"
	"github.com/photowey/disruptor.go/pkg/runtimegraph"
	"github.com/photowey/disruptor.go/pkg/runtimevars"
)

type routeEvent struct {
	Value int64
}

type routeEventFactory struct{}

func (routeEventFactory) NewEvent() routeEvent {
	return routeEvent{}
}

type routeTranslator struct {
	value int64
}

func (t routeTranslator) Translate(request disruptor.TranslateRequest[routeEvent]) {
	request.Event.Value = t.value
}

type decideRouteHandler struct {
	steps chan<- string
}

func (h decideRouteHandler) OnEvent(request event.Request[routeEvent]) error {
	if err := request.Runtime.Set("route.fast", true); err != nil {
		return err
	}
	if err := request.Runtime.Set("route.audit", false); err != nil {
		return err
	}
	h.steps <- fmt.Sprintf("route:%d", request.Event.Value)

	return nil
}

type routeStepHandler struct {
	name  string
	steps chan<- string
}

func (h routeStepHandler) OnEvent(request event.Request[routeEvent]) error {
	h.steps <- fmt.Sprintf("%s:%d", h.name, request.Event.Value)
	return nil
}

type routeVariablesProvider struct{}

func (routeVariablesProvider) Variables(request runtimevars.ProviderRequest[routeEvent]) (runtimevars.Variables, error) {
	return routeVariables{
		premium: request.Event != nil && request.Event.Value > 10,
	}, nil
}

type routeVariables struct {
	premium bool
}

func (v routeVariables) Lookup(path string) (any, bool) {
	switch path {
	case "plan.premium":
		return v.premium, true
	default:
		return nil, false
	}
}

func (v routeVariables) LookupValue(path string) (runtimevars.TypedValue, bool, error) {
	switch path {
	case "plan.premium":
		return runtimevars.TypedValue{
			Kind: runtimevars.TypedValueBool,
			Bool: v.premium,
		}, true, nil
	default:
		return runtimevars.TypedValue{}, false, nil
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d, err := disruptor.New(routeEventFactory{}, 1024)
	if err != nil {
		panic(err)
	}

	steps := make(chan string, 2)
	graph := runtimegraph.MustRuntimeGraph[routeEvent]("runtime-route").
		MustNode("route", decideRouteHandler{steps: steps}).
		MustNode("fast", routeStepHandler{name: "fast", steps: steps}).
		MustNode("audit", routeStepHandler{name: "audit", steps: steps}).
		MustEdge(topology.StartNode, "route").
		MustEdge("route", "fast", runtimegraph.WhenExpression[routeEvent](`${plan.premium} && ${route.fast}`)).
		MustEdge("route", "audit", runtimegraph.WhenExpression[routeEvent](`${route.audit}`)).
		MustEdge("fast", topology.EndNode).
		MustEdge("audit", topology.EndNode)

	if _, err := d.HandleRuntimeGraph(
		graph,
		disruptor.WithRuntimeGraphVariablesProvider[routeEvent](routeVariablesProvider{}),
	); err != nil {
		panic(err)
	}
	if err := d.Start(ctx); err != nil {
		panic(err)
	}

	if err := d.RingBuffer().PublishEvent(ctx, routeTranslator{value: 11}); err != nil {
		panic(err)
	}

	handled := []string{<-steps, <-steps}
	fmt.Println(strings.Join(handled, ","))

	d.Stop()
	if err := d.Wait(); err != nil {
		panic(err)
	}
}
