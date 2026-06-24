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

package disruptor_test

import (
	"testing"

	"github.com/photowey/disruptor.go/pkg/disruptor"
)

func TestRuntimeExpressionEvaluatesPathsComparisonsAndBitwise(t *testing.T) {
	bag := disruptor.NewRuntimeBag()
	mustSetRuntimeValue(t, bag, "approved", true)
	mustSetRuntimeValue(t, bag, "risk.score", int64(91))
	mustSetRuntimeValue(t, bag, "flags", uint64(0b101))
	mustSetRuntimeValue(t, bag, "status", "paid")

	tests := []struct {
		name       string
		expression disruptor.RuntimeExpression
		want       bool
	}{
		{name: "bool path", expression: `${approved}`, want: true},
		{name: "comparison", expression: `${risk.score} >= 90`, want: true},
		{name: "string comparison", expression: `${status} == "paid"`, want: true},
		{name: "top-level bitwise true", expression: `${flags} & 1`, want: true},
		{name: "top-level bitwise false", expression: `${flags} & 2`, want: false},
		{name: "logical grouping", expression: `${approved} && (${risk.score} > 80)`, want: true},
	}

	compiler := disruptor.NewRuntimeExpressionCompiler()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expression, err := compiler.Compile(tt.expression)
			if err != nil {
				t.Fatalf("compile: %v", err)
			}
			got, err := expression.EvaluateBool(disruptor.ExpressionRequest{
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

func TestRuntimeExpressionRejectsIntermediateIntegerAsBool(t *testing.T) {
	bag := disruptor.NewRuntimeBag()
	mustSetRuntimeValue(t, bag, "flags", int64(1))
	mustSetRuntimeValue(t, bag, "vip", true)

	compiler := disruptor.NewRuntimeExpressionCompiler()
	expression, err := compiler.Compile(`(${flags} & 1) && ${vip}`)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	_, err = expression.EvaluateBool(disruptor.ExpressionRequest{
		Variables: bag,
	})
	if err == nil {
		t.Fatal("expected intermediate integer bool conversion error")
	}
}

func mustSetRuntimeValue(t *testing.T, bag disruptor.RuntimeBag, path string, value any) {
	t.Helper()

	if err := bag.Set(path, value); err != nil {
		t.Fatalf("set %s: %v", path, err)
	}
}
