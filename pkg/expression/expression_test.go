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

package expression

import (
	"errors"
	"math"
	"strconv"
	"strings"
	"testing"

	runtimevars "github.com/photowey/disruptor.go/pkg/runtimevars"
)

func TestExpressionEvaluatesPathsComparisonsAndBitwise(t *testing.T) {
	bag := runtimevars.NewBag()
	mustSetRuntimeValue(t, bag, "approved", true)
	mustSetRuntimeValue(t, bag, "risk.score", int64(91))
	mustSetRuntimeValue(t, bag, "flags", uint64(0b101))
	mustSetRuntimeValue(t, bag, "status", "paid")
	mustSetRuntimeValue(t, bag, "ratio", float64(1.25))
	mustSetRuntimeValue(t, bag, "limit", uint64(100))
	mustSetRuntimeValue(t, bag, "missing.value", nil)

	tests := []struct {
		name       string
		expression Expression
		want       bool
	}{
		{name: "bool path", expression: `${approved}`, want: true},
		{name: "comparison", expression: `${risk.score} >= 90`, want: true},
		{name: "uint comparison", expression: `${limit} == 100`, want: true},
		{name: "float comparison", expression: `${ratio} > 1.20`, want: true},
		{name: "string comparison", expression: `${status} == "paid"`, want: true},
		{name: "nil comparison", expression: `${missing.value} == nil`, want: true},
		{name: "top-level bitwise true", expression: `${flags} & 1`, want: true},
		{name: "top-level bitwise false", expression: `${flags} & 2`, want: false},
		{name: "logical grouping", expression: `${approved} && (${risk.score} > 80)`, want: true},
	}

	compiler := NewCompiler()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expression, err := compiler.Compile(tt.expression)
			if err != nil {
				t.Fatalf("compile: %v", err)
			}
			got, err := expression.EvaluateBool(Request{
				Variables: bag,
			})
			if err != nil {
				t.Fatalf("evaluate: %v", err)
			}
			if got != tt.want {
				t.Fatalf("evaluate = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExpressionRejectsIntermediateIntegerAsBool(t *testing.T) {
	bag := runtimevars.NewBag()
	mustSetRuntimeValue(t, bag, "flags", int64(1))
	mustSetRuntimeValue(t, bag, "vip", true)

	compiler := NewCompiler()
	expression, err := compiler.Compile(`(${flags} & 1) && ${vip}`)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	_, err = expression.EvaluateBool(Request{
		Variables: bag,
	})
	if err == nil {
		t.Fatal("expected intermediate integer bool conversion error")
	}
}

func TestExpressionComparesLargeIntegersExactly(t *testing.T) {
	t.Parallel()

	bag := runtimevars.NewBag()
	mustSetRuntimeValue(t, bag, "left", int64(9_007_199_254_740_993))
	mustSetRuntimeValue(t, bag, "right", int64(9_007_199_254_740_992))
	mustSetRuntimeValue(t, bag, "unsigned", uint64(9_007_199_254_740_993))

	tests := []struct {
		name       string
		expression Expression
		want       bool
	}{
		{name: "signed greater than signed", expression: `${left} > ${right}`, want: true},
		{name: "signed not equal signed", expression: `${left} != ${right}`, want: true},
		{name: "unsigned equals signed", expression: `${unsigned} == ${left}`, want: true},
		{name: "unsigned greater than signed", expression: `${unsigned} > ${right}`, want: true},
	}

	compiler := NewCompiler()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expression, err := compiler.Compile(tt.expression)
			if err != nil {
				t.Fatalf("compile: %v", err)
			}
			got, err := expression.EvaluateBool(Request{
				Variables: bag,
			})
			if err != nil {
				t.Fatalf("evaluate: %v", err)
			}
			if got != tt.want {
				t.Fatalf("evaluate = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExpressionUsesTypedVariableLookup(t *testing.T) {
	t.Parallel()

	compiler := NewCompiler()
	expression, err := compiler.Compile(`${value} >= 40`)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	got, err := expression.EvaluateBool(Request{
		Variables: typedExpressionVariables{},
	})
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	if !got {
		t.Fatalf("evaluate = false, want true")
	}
}

func TestExpressionNumberAdapterConvertsAndCompares(t *testing.T) {
	t.Parallel()

	compiler := NewCompiler(WithNumberAdapter(testDecimalAdapter{}))

	tests := []struct {
		name       string
		expression Expression
		variables  runtimevars.Variables
		want       bool
	}{
		{
			name:       "ordinary value number greater than string",
			expression: `${amount} > "10.50"`,
			variables:  testDecimalBag("amount", testDecimalRaw{cents: 1125}),
			want:       true,
		},
		{
			name:       "string less than value number",
			expression: `"10.50" < ${amount}`,
			variables:  testDecimalBag("amount", testDecimalRaw{cents: 1125}),
			want:       true,
		},
		{
			name:       "value number greater than builtin int",
			expression: `${amount} > 10`,
			variables:  testDecimalBag("amount", testDecimalRaw{cents: 1125}),
			want:       true,
		},
		{
			name:       "typed object reaches number adapter",
			expression: `${amount} >= "11.25"`,
			variables:  typedDecimalVariables{path: "amount", value: testDecimalRaw{cents: 1125}},
			want:       true,
		},
		{
			name:       "value number equals value number",
			expression: `${left} == ${right}`,
			variables:  testDecimalPairBag("left", testDecimalRaw{cents: 1125}, "right", testDecimalRaw{cents: 1125}),
			want:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expression, err := compiler.Compile(tt.expression)
			if err != nil {
				t.Fatalf("compile: %v", err)
			}
			got, err := expression.EvaluateBool(Request{
				Variables: tt.variables,
			})
			if err != nil {
				t.Fatalf("evaluate: %v", err)
			}
			if got != tt.want {
				t.Fatalf("evaluate = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExpressionNumberAdapterOrdering(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		options []CompilerOption
		want    bool
	}{
		{
			name: "lower order wins",
			options: []CompilerOption{
				WithNumberAdapter(orderedNumberAdapter{name: "late", order: 10, result: -1}),
				WithNumberAdapter(orderedNumberAdapter{name: "early", order: -10, result: 1}),
			},
			want: true,
		},
		{
			name: "equal order keeps registration order",
			options: []CompilerOption{
				WithNumberAdapter(orderedNumberAdapter{name: "first", order: 0, result: 1}),
				WithNumberAdapter(orderedNumberAdapter{name: "second", order: 0, result: -1}),
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compiler := NewCompiler(tt.options...)
			expression, err := compiler.Compile(`${value} > 0`)
			if err != nil {
				t.Fatalf("compile: %v", err)
			}
			got, err := expression.EvaluateBool(Request{
				Variables: testDecimalBag("value", orderSensitiveRaw{}),
			})
			if err != nil {
				t.Fatalf("evaluate: %v", err)
			}
			if got != tt.want {
				t.Fatalf("evaluate = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExpressionNumberAdapterErrorStopsEvaluation(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("decimal compare failed")
	var laterCalls int
	compiler := NewCompiler(
		WithNumberAdapter(errorDecimalAdapter{err: wantErr}),
		WithNumberAdapter(countingNumberAdapter{calls: &laterCalls}),
	)
	expression, err := compiler.Compile(`${amount} > "10.50"`)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	_, err = expression.EvaluateBool(Request{
		Variables: testDecimalBag("amount", testDecimalRaw{cents: 1125}),
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("evaluate error = %v, want %v", err, wantErr)
	}
	if laterCalls != 0 {
		t.Fatalf("later adapter calls = %d, want 0", laterCalls)
	}
}

func TestExpressionNumberAdapterBoolConversionIsFinalOnly(t *testing.T) {
	t.Parallel()

	compiler := NewCompiler(WithNumberAdapter(testDecimalAdapter{}))
	finalExpression, err := compiler.Compile(`${amount}`)
	if err != nil {
		t.Fatalf("compile final: %v", err)
	}
	got, err := finalExpression.EvaluateBool(Request{
		Variables: testDecimalBag("amount", testDecimalRaw{cents: 1}),
	})
	if err != nil {
		t.Fatalf("evaluate final: %v", err)
	}
	if !got {
		t.Fatal("evaluate final = false, want true")
	}

	for _, expressionText := range []Expression{`${amount} && true`, `!${amount}`} {
		expression, err := compiler.Compile(expressionText)
		if err != nil {
			t.Fatalf("compile intermediate %s: %v", expressionText, err)
		}
		_, err = expression.EvaluateBool(Request{
			Variables: testDecimalBag("amount", testDecimalRaw{cents: 1}),
		})
		if err == nil {
			t.Fatalf("evaluate %s error is nil, want logical bool error", expressionText)
		}
	}
}

func TestExpressionNumberAdapterDoesNotInterceptBuiltinFastPath(t *testing.T) {
	t.Parallel()

	compiler := NewCompiler(WithNumberAdapter(failingFastPathAdapter{}))
	expression, err := compiler.Compile(`${left} < 2`)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	got, err := expression.EvaluateBool(Request{
		Variables: testDecimalBag("left", int64(1)),
	})
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	if !got {
		t.Fatal("evaluate = false, want true")
	}
}

type typedExpressionVariables struct{}

func (typedExpressionVariables) Lookup(path string) (any, bool) {
	panic("Lookup should not be called when LookupValue is available")
}

func (typedExpressionVariables) LookupValue(path string) (runtimevars.TypedValue, bool, error) {
	if path != "value" {
		return runtimevars.TypedValue{}, false, nil
	}

	return runtimevars.TypedValue{
		Kind: runtimevars.TypedValueInt,
		Int:  41,
	}, true, nil
}

func mustSetRuntimeValue(t *testing.T, bag *runtimevars.Bag, path string, value any) {
	t.Helper()

	if err := bag.Set(path, value); err != nil {
		t.Fatalf("set %s: %v", path, err)
	}
}

type testDecimalRaw struct {
	cents int64
}

type testDecimalNumber struct {
	cents int64
}

func (n testDecimalNumber) NumberKind() NumberKind {
	return "test.decimal"
}

type testDecimalAdapter struct{}

func (testDecimalAdapter) Convert(request ValueConvertRequest) (Value, bool, error) {
	value, ok := request.Value.(testDecimalRaw)
	if !ok {
		return Value{}, false, nil
	}

	return Value{
		Kind:   ValueNumber,
		Number: testDecimalNumber(value),
	}, true, nil
}

func (testDecimalAdapter) CompareNumber(
	request NumberCompareRequest,
) (int, bool, error) {
	left, ok := testDecimalFromValue(request.Left)
	if !ok {
		return 0, false, nil
	}
	right, ok := testDecimalFromValue(request.Right)
	if !ok {
		return 0, false, nil
	}

	return compareTestDecimal(left, right), true, nil
}

func (testDecimalAdapter) ConvertNumberToBool(
	request NumberBoolRequest,
) (bool, bool, error) {
	value, ok := testDecimalFromValue(request.Value)
	if !ok {
		return false, false, nil
	}

	return value.cents != 0, true, nil
}

type errorDecimalAdapter struct {
	err error
}

func (a errorDecimalAdapter) Convert(request ValueConvertRequest) (Value, bool, error) {
	return testDecimalAdapter{}.Convert(request)
}

func (a errorDecimalAdapter) CompareNumber(
	request NumberCompareRequest,
) (int, bool, error) {
	if _, ok := testDecimalFromValue(request.Left); !ok {
		return 0, false, nil
	}

	return 0, true, a.err
}

func (a errorDecimalAdapter) ConvertNumberToBool(
	request NumberBoolRequest,
) (bool, bool, error) {
	return testDecimalAdapter{}.ConvertNumberToBool(request)
}

type countingNumberAdapter struct {
	calls *int
}

func (a countingNumberAdapter) Convert(request ValueConvertRequest) (Value, bool, error) {
	return Value{}, false, nil
}

func (a countingNumberAdapter) CompareNumber(
	request NumberCompareRequest,
) (int, bool, error) {
	*a.calls = *a.calls + 1
	return 0, false, nil
}

func (a countingNumberAdapter) ConvertNumberToBool(
	request NumberBoolRequest,
) (bool, bool, error) {
	return false, false, nil
}

type failingFastPathAdapter struct{}

func (failingFastPathAdapter) Convert(request ValueConvertRequest) (Value, bool, error) {
	return Value{}, false, errors.New("adapter should not convert builtin values")
}

func (failingFastPathAdapter) CompareNumber(
	request NumberCompareRequest,
) (int, bool, error) {
	return 0, false, errors.New("adapter should not compare builtin values")
}

func (failingFastPathAdapter) ConvertNumberToBool(
	request NumberBoolRequest,
) (bool, bool, error) {
	return false, false, errors.New("adapter should not convert builtin bool results")
}

type orderSensitiveRaw struct{}

type orderSensitiveNumber struct {
	kind   NumberKind
	result int
}

func (n orderSensitiveNumber) NumberKind() NumberKind {
	return n.kind
}

type orderedNumberAdapter struct {
	name   string
	order  int
	result int
}

func (a orderedNumberAdapter) Order() int {
	return a.order
}

func (a orderedNumberAdapter) Convert(request ValueConvertRequest) (Value, bool, error) {
	if _, ok := request.Value.(orderSensitiveRaw); !ok {
		return Value{}, false, nil
	}

	return Value{
		Kind: ValueNumber,
		Number: orderSensitiveNumber{
			kind:   NumberKind(a.name),
			result: a.result,
		},
	}, true, nil
}

func (a orderedNumberAdapter) CompareNumber(
	request NumberCompareRequest,
) (int, bool, error) {
	value, ok := request.Left.Number.(orderSensitiveNumber)
	if !ok || value.kind != NumberKind(a.name) {
		return 0, false, nil
	}

	return value.result, true, nil
}

func (a orderedNumberAdapter) ConvertNumberToBool(
	request NumberBoolRequest,
) (bool, bool, error) {
	return false, false, nil
}

type typedDecimalVariables struct {
	path  string
	value testDecimalRaw
}

func (v typedDecimalVariables) Lookup(path string) (any, bool) {
	return nil, false
}

func (v typedDecimalVariables) LookupValue(
	path string,
) (runtimevars.TypedValue, bool, error) {
	if path != v.path {
		return runtimevars.TypedValue{}, false, nil
	}

	return runtimevars.TypedValue{
		Kind:  runtimevars.TypedValueObject,
		Value: v.value,
	}, true, nil
}

func testDecimalBag(path string, value any) *runtimevars.Bag {
	bag := runtimevars.NewBag()
	if err := bag.Set(path, value); err != nil {
		panic(err)
	}

	return bag
}

func testDecimalPairBag(
	leftPath string,
	leftValue any,
	rightPath string,
	rightValue any,
) *runtimevars.Bag {
	bag := runtimevars.NewBag()
	if err := bag.Set(leftPath, leftValue); err != nil {
		panic(err)
	}
	if err := bag.Set(rightPath, rightValue); err != nil {
		panic(err)
	}

	return bag
}

func testDecimalFromValue(value Value) (testDecimalNumber, bool) {
	switch value.Kind {
	case ValueNumber:
		number, ok := value.Number.(testDecimalNumber)
		return number, ok
	case ValueString:
		number, err := parseTestDecimal(expressionString(value))
		return number, err == nil
	case ValueInt:
		integer := expressionInt(value)
		if integer > math.MaxInt64/100 || integer < math.MinInt64/100 {
			return testDecimalNumber{}, false
		}

		return testDecimalNumber{cents: integer * 100}, true
	case ValueUint:
		unsigned := expressionUint(value)
		if unsigned > uint64(math.MaxInt64/100) {
			return testDecimalNumber{}, false
		}

		return testDecimalNumber{cents: int64(unsigned) * 100}, true
	default:
		return testDecimalNumber{}, false
	}
}

func parseTestDecimal(raw string) (testDecimalNumber, error) {
	whole, fraction, ok := strings.Cut(raw, ".")
	if !ok {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return testDecimalNumber{}, err
		}

		return testDecimalNumber{cents: value * 100}, nil
	}
	if len(fraction) != 2 {
		return testDecimalNumber{}, strconv.ErrSyntax
	}
	wholeValue, err := strconv.ParseInt(whole, 10, 64)
	if err != nil {
		return testDecimalNumber{}, err
	}
	fractionValue, err := strconv.ParseInt(fraction, 10, 64)
	if err != nil {
		return testDecimalNumber{}, err
	}

	return testDecimalNumber{cents: wholeValue*100 + fractionValue}, nil
}

func compareTestDecimal(left testDecimalNumber, right testDecimalNumber) int {
	switch {
	case left.cents < right.cents:
		return -1
	case left.cents > right.cents:
		return 1
	default:
		return 0
	}
}
