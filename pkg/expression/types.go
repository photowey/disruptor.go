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
	"context"
	"errors"

	runtimevars "github.com/photowey/disruptor.go/pkg/runtimevars"
)

var ErrInvalid = errors.New("expression: invalid expression")

// Expression is a compiled-at-registration runtime graph expression.
type Expression string

// Compiler compiles runtime expressions into bool evaluators.
type Compiler interface {
	Compile(expression Expression) (BoolExpression, error)
}

// BoolExpression evaluates an expression and converts the final result to bool.
type BoolExpression interface {
	EvaluateBool(request Request) (bool, error)
}

// Request supplies variables to an expression evaluation.
type Request struct {
	Context   context.Context
	Variables runtimevars.Variables
}

// ValueKind identifies an expression value type.
type ValueKind uint8

const (
	// ValueInvalid is the zero value and is never produced intentionally.
	ValueInvalid ValueKind = iota
	// ValueBool represents a bool value.
	ValueBool
	// ValueInt represents a signed integer value.
	ValueInt
	// ValueUint represents an unsigned integer value.
	ValueUint
	// ValueFloat represents a floating point value.
	ValueFloat
	// ValueString represents a string value.
	ValueString
	// ValueObject represents an unsupported object value.
	ValueObject
	// ValueNil represents nil.
	ValueNil
)

// Value is the evaluator's normalized value representation.
type Value struct {
	Kind   ValueKind
	Value  any
	Bool   bool
	Int    int64
	Uint   uint64
	Float  float64
	String string
}

// ValueConvertRequest describes a converter request.
type ValueConvertRequest struct {
	Value any
}

// ValueConverter converts raw values into expression values.
type ValueConverter interface {
	Convert(request ValueConvertRequest) (Value, bool, error)
}

// ValueConverterFunc adapts a function to ValueConverter.
type ValueConverterFunc func(
	request ValueConvertRequest,
) (Value, bool, error)

// Convert calls the wrapped converter function.
func (fn ValueConverterFunc) Convert(
	request ValueConvertRequest,
) (Value, bool, error) {
	if fn == nil {
		return Value{}, false, nil
	}

	return fn(request)
}
