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

package disruptor

import (
	"context"
	"fmt"

	internalexpr "github.com/photowey/disruptor.go/internal/expression"
)

// RuntimeExpression is a compiled-at-registration runtime graph expression.
type RuntimeExpression string

// ExpressionCompiler compiles runtime expressions into bool evaluators.
type ExpressionCompiler interface {
	Compile(expression RuntimeExpression) (BoolExpression, error)
}

// BoolExpression evaluates an expression and converts the final result to bool.
type BoolExpression interface {
	EvaluateBool(request ExpressionRequest) (bool, error)
}

// ExpressionRequest supplies variables to an expression evaluation.
type ExpressionRequest struct {
	Context   context.Context
	Variables RuntimeVariables
}

// ExpressionValueKind identifies an expression value type.
type ExpressionValueKind uint8

const (
	// ExpressionValueInvalid is the zero value and is never produced intentionally.
	ExpressionValueInvalid ExpressionValueKind = iota
	// ExpressionValueBool represents a bool value.
	ExpressionValueBool
	// ExpressionValueInt represents a signed integer value.
	ExpressionValueInt
	// ExpressionValueUint represents an unsigned integer value.
	ExpressionValueUint
	// ExpressionValueFloat represents a floating point value.
	ExpressionValueFloat
	// ExpressionValueString represents a string value.
	ExpressionValueString
	// ExpressionValueObject represents an unsupported object value.
	ExpressionValueObject
	// ExpressionValueNil represents nil.
	ExpressionValueNil
)

// ExpressionValue is the evaluator's normalized value representation.
type ExpressionValue struct {
	Kind  ExpressionValueKind
	Value any
}

// ExpressionValueConvertRequest describes a converter request.
type ExpressionValueConvertRequest struct {
	Value any
}

// ExpressionValueConverter converts raw values into expression values.
type ExpressionValueConverter interface {
	Convert(request ExpressionValueConvertRequest) (ExpressionValue, bool, error)
}

// ExpressionValueConverterFunc adapts a function to ExpressionValueConverter.
type ExpressionValueConverterFunc func(
	request ExpressionValueConvertRequest,
) (ExpressionValue, bool, error)

// Convert calls the wrapped converter function.
func (fn ExpressionValueConverterFunc) Convert(
	request ExpressionValueConvertRequest,
) (ExpressionValue, bool, error) {
	if fn == nil {
		return ExpressionValue{}, false, nil
	}

	return fn(request)
}

// RuntimeExpressionCompilerOption configures the default expression compiler.
type RuntimeExpressionCompilerOption interface {
	applyRuntimeExpressionCompiler(config *runtimeExpressionCompilerConfig) error
}

type runtimeExpressionCompilerConfig struct {
	options []internalexpr.RuntimeExpressionCompilerOption
}

type runtimeExpressionCompilerOptionFunc struct {
	applyFunc func(*runtimeExpressionCompilerConfig) error
}

//nolint:unused // The method satisfies RuntimeExpressionCompilerOption and is called through the interface.
func (fn runtimeExpressionCompilerOptionFunc) applyRuntimeExpressionCompiler(
	config *runtimeExpressionCompilerConfig,
) error {
	return fn.applyFunc(config)
}

// WithExpressionValueConverter adds a custom expression value converter.
func WithExpressionValueConverter(
	converter ExpressionValueConverter,
) RuntimeExpressionCompilerOption {
	return runtimeExpressionCompilerOptionFunc{
		applyFunc: func(config *runtimeExpressionCompilerConfig) error {
			if converter == nil {
				return fmt.Errorf("%w: expression value converter is nil", ErrInvalidRuntimeExpression)
			}

			config.options = append(
				config.options,
				internalexpr.WithExpressionValueConverter(
					expressionValueConverterAdapter{converter: converter},
				),
			)
			return nil
		},
	}
}

// NewRuntimeExpressionCompiler creates the default runtime expression compiler.
func NewRuntimeExpressionCompiler(
	opts ...RuntimeExpressionCompilerOption,
) ExpressionCompiler {
	config := runtimeExpressionCompilerConfig{}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt.applyRuntimeExpressionCompiler(&config); err != nil {
			panic(err)
		}
	}

	return runtimeExpressionCompiler{
		inner: internalexpr.NewRuntimeExpressionCompiler(config.options...),
	}
}

type runtimeExpressionCompiler struct {
	inner internalexpr.ExpressionCompiler
}

func (c runtimeExpressionCompiler) Compile(
	expression RuntimeExpression,
) (BoolExpression, error) {
	compiled, err := c.inner.Compile(internalexpr.RuntimeExpression(expression))
	if err != nil {
		return nil, err
	}

	return runtimeBoolExpression{inner: compiled}, nil
}

type runtimeBoolExpression struct {
	inner internalexpr.BoolExpression
}

func (e runtimeBoolExpression) EvaluateBool(request ExpressionRequest) (bool, error) {
	return e.inner.EvaluateBool(internalexpr.ExpressionRequest{
		Context:   request.Context,
		Variables: request.Variables,
	})
}

type expressionValueConverterAdapter struct {
	converter ExpressionValueConverter
}

func (a expressionValueConverterAdapter) Convert(
	request internalexpr.ExpressionValueConvertRequest,
) (internalexpr.ExpressionValue, bool, error) {
	if a.converter == nil {
		return internalexpr.ExpressionValue{}, false, nil
	}

	value, handled, err := a.converter.Convert(ExpressionValueConvertRequest{
		Value: request.Value,
	})
	if err != nil || !handled {
		return internalexpr.ExpressionValue{}, handled, err
	}

	return internalexpr.ExpressionValue{
		Kind:  internalexpr.ExpressionValueKind(value.Kind),
		Value: value.Value,
	}, true, nil
}
