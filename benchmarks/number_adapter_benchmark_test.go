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

package benchmarks

import (
	"strconv"
	"strings"
	"testing"

	"github.com/photowey/disruptor.go/pkg/expression"
	"github.com/photowey/disruptor.go/pkg/runtimevars"
)

func BenchmarkExpressionNumberAdapterComparison(b *testing.B) {
	amount := benchDecimalRaw{cents: 1125}
	compiler := expression.NewCompiler(
		expression.WithNumberAdapter(benchDecimalAdapter{}),
	)
	compiled, err := compiler.Compile(`${amount} > "10.50"`)
	if err != nil {
		b.Fatalf("compile: %v", err)
	}
	request := expression.Request{
		Variables: benchDecimalVariables{amount: &amount},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		got, err := compiled.EvaluateBool(request)
		if err != nil {
			b.Fatalf("evaluate: %v", err)
		}
		if !got {
			b.Fatal("evaluate = false, want true")
		}
	}
}

type benchDecimalVariables struct {
	amount *benchDecimalRaw
}

func (v benchDecimalVariables) Lookup(path string) (any, bool) {
	return nil, false
}

func (v benchDecimalVariables) LookupValue(
	path string,
) (runtimevars.TypedValue, bool, error) {
	if path != "amount" {
		return runtimevars.TypedValue{}, false, nil
	}

	return runtimevars.TypedValue{
		Kind:  runtimevars.TypedValueObject,
		Value: v.amount,
	}, true, nil
}

type benchDecimalRaw struct {
	cents int64
}

type benchDecimalNumber struct {
	cents int64
}

func (n benchDecimalNumber) NumberKind() expression.NumberKind {
	return "bench.decimal"
}

type benchDecimalAdapter struct{}

func (benchDecimalAdapter) Convert(
	request expression.ValueConvertRequest,
) (expression.Value, bool, error) {
	switch value := request.Value.(type) {
	case *benchDecimalRaw:
		return expression.Value{
			Kind:   expression.ValueNumber,
			Number: (*benchDecimalNumber)(value),
		}, true, nil
	case benchDecimalRaw:
		return expression.Value{
			Kind:   expression.ValueNumber,
			Number: benchDecimalNumber(value),
		}, true, nil
	default:
		return expression.Value{}, false, nil
	}
}

func (benchDecimalAdapter) CompareNumber(
	request expression.NumberCompareRequest,
) (int, bool, error) {
	left, ok := benchDecimalFromValue(request.Left)
	if !ok {
		return 0, false, nil
	}
	right, ok := benchDecimalFromValue(request.Right)
	if !ok {
		return 0, false, nil
	}

	return compareBenchDecimal(left, right), true, nil
}

func (benchDecimalAdapter) ConvertNumberToBool(
	request expression.NumberBoolRequest,
) (bool, bool, error) {
	value, ok := benchDecimalFromValue(request.Value)
	if !ok {
		return false, false, nil
	}

	return value.cents != 0, true, nil
}

func benchDecimalFromValue(value expression.Value) (benchDecimalNumber, bool) {
	switch value.Kind {
	case expression.ValueNumber:
		return benchDecimalFromNumber(value.Number)
	case expression.ValueString:
		number, err := parseBenchDecimal(value.String)
		return number, err == nil
	default:
		return benchDecimalNumber{}, false
	}
}

func benchDecimalFromNumber(number expression.Number) (benchDecimalNumber, bool) {
	switch typed := number.(type) {
	case benchDecimalNumber:
		return typed, true
	case *benchDecimalNumber:
		if typed == nil {
			return benchDecimalNumber{}, false
		}

		return *typed, true
	default:
		return benchDecimalNumber{}, false
	}
}

func parseBenchDecimal(raw string) (benchDecimalNumber, error) {
	whole, fraction, ok := strings.Cut(raw, ".")
	if !ok {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return benchDecimalNumber{}, err
		}

		return benchDecimalNumber{cents: value * 100}, nil
	}
	if len(fraction) != 2 {
		return benchDecimalNumber{}, strconv.ErrSyntax
	}
	wholeValue, err := strconv.ParseInt(whole, 10, 64)
	if err != nil {
		return benchDecimalNumber{}, err
	}
	fractionValue, err := strconv.ParseInt(fraction, 10, 64)
	if err != nil {
		return benchDecimalNumber{}, err
	}

	return benchDecimalNumber{cents: wholeValue*100 + fractionValue}, nil
}

func compareBenchDecimal(left benchDecimalNumber, right benchDecimalNumber) int {
	switch {
	case left.cents < right.cents:
		return -1
	case left.cents > right.cents:
		return 1
	default:
		return 0
	}
}
