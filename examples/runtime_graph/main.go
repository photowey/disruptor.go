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
	"strconv"
	"strings"

	"github.com/photowey/disruptor.go/pkg/disruptor"
	"github.com/photowey/disruptor.go/pkg/event"
	"github.com/photowey/disruptor.go/pkg/expression"
	topology "github.com/photowey/disruptor.go/pkg/graph"
	"github.com/photowey/disruptor.go/pkg/runtimegraph"
	"github.com/photowey/disruptor.go/pkg/runtimevars"
)

type routeEvent struct {
	Value int64
}

type routeAmount struct {
	cents int64
}

type routeAmountNumber struct {
	cents int64
}

func (n routeAmountNumber) NumberKind() expression.NumberKind {
	return "example.amount"
}

type routeAmountAdapter struct{}

func (routeAmountAdapter) Convert(
	request expression.ValueConvertRequest,
) (expression.Value, bool, error) {
	value, ok := request.Value.(routeAmount)
	if !ok {
		return expression.Value{}, false, nil
	}

	return expression.Value{
		Kind:   expression.ValueNumber,
		Number: routeAmountNumber(value),
	}, true, nil
}

func (routeAmountAdapter) CompareNumber(
	request expression.NumberCompareRequest,
) (int, bool, error) {
	left, ok := routeAmountFromValue(request.Left)
	if !ok {
		return 0, false, nil
	}
	right, ok := routeAmountFromValue(request.Right)
	if !ok {
		return 0, false, nil
	}

	return compareRouteAmount(left, right), true, nil
}

func (routeAmountAdapter) ConvertNumberToBool(
	request expression.NumberBoolRequest,
) (bool, bool, error) {
	value, ok := routeAmountFromValue(request.Value)
	if !ok {
		return false, false, nil
	}

	return value.cents != 0, true, nil
}

func routeAmountFromValue(value expression.Value) (routeAmountNumber, bool) {
	switch value.Kind {
	case expression.ValueNumber:
		number, ok := value.Number.(routeAmountNumber)
		return number, ok
	case expression.ValueString:
		number, err := parseRouteAmount(value.String)
		return number, err == nil
	default:
		return routeAmountNumber{}, false
	}
}

func parseRouteAmount(raw string) (routeAmountNumber, error) {
	whole, fraction, ok := strings.Cut(raw, ".")
	if !ok {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return routeAmountNumber{}, err
		}

		return routeAmountNumber{cents: value * 100}, nil
	}
	if len(fraction) != 2 {
		return routeAmountNumber{}, strconv.ErrSyntax
	}
	wholeValue, err := strconv.ParseInt(whole, 10, 64)
	if err != nil {
		return routeAmountNumber{}, err
	}
	fractionValue, err := strconv.ParseInt(fraction, 10, 64)
	if err != nil {
		return routeAmountNumber{}, err
	}

	return routeAmountNumber{cents: wholeValue*100 + fractionValue}, nil
}

func compareRouteAmount(left routeAmountNumber, right routeAmountNumber) int {
	switch {
	case left.cents < right.cents:
		return -1
	case left.cents > right.cents:
		return 1
	default:
		return 0
	}
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

func (routeVariablesProvider) Variables(
	request runtimevars.ProviderRequest[routeEvent],
) (runtimevars.Variables, error) {
	var amount routeAmount
	if request.Event != nil {
		amount = routeAmount{cents: request.Event.Value * 100}
	}

	return routeVariables{
		premium: request.Event != nil && request.Event.Value > 10,
		amount:  amount,
	}, nil
}

type routeVariables struct {
	premium bool
	amount  routeAmount
}

func (v routeVariables) Lookup(path string) (any, bool) {
	switch path {
	case "plan.premium":
		return v.premium, true
	case "plan.amount":
		return v.amount, true
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
	case "plan.amount":
		return runtimevars.TypedValue{
			Kind:  runtimevars.TypedValueObject,
			Value: v.amount,
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
	compiler := expression.NewCompiler(
		expression.WithNumberAdapter(routeAmountAdapter{}),
	)
	graph := runtimegraph.MustRuntimeGraph[routeEvent](
		"runtime-route",
		runtimegraph.WithExpressionCompiler(compiler),
	).
		MustNode("route", decideRouteHandler{steps: steps}).
		MustNode("fast", routeStepHandler{name: "fast", steps: steps}).
		MustNode("audit", routeStepHandler{name: "audit", steps: steps}).
		MustEdge(topology.StartNode, "route").
		MustEdge(
			"route",
			"fast",
			runtimegraph.WhenExpression[routeEvent](
				`${plan.premium} && ${plan.amount} > "10.00" && ${route.fast}`,
			),
		).
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
