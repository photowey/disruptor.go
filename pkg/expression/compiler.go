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
	"fmt"

	"github.com/photowey/disruptor.go/pkg/runtimevars"
)

type CompilerOption interface {
	applyCompiler(config *compilerConfig) error
}

type compilerConfig struct {
	converters []ValueConverter
}

type compilerOptionFunc struct {
	applyFunc func(*compilerConfig) error
}

//nolint:unused // The method satisfies CompilerOption and is called through the interface.
func (fn compilerOptionFunc) applyCompiler(
	config *compilerConfig,
) error {
	return fn.applyFunc(config)
}

// WithValueConverter adds a custom expression value converter.
func WithValueConverter(
	converter ValueConverter,
) CompilerOption {
	return compilerOptionFunc{
		applyFunc: func(config *compilerConfig) error {
			if converter == nil {
				return fmt.Errorf("%w: expression value converter is nil", ErrInvalid)
			}

			config.converters = append(config.converters, converter)
			return nil
		},
	}
}

// NewCompiler creates the default runtime expression compiler.
func NewCompiler(
	opts ...CompilerOption,
) Compiler {
	config := compilerConfig{
		converters: defaultValueConverters(),
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt.applyCompiler(&config); err != nil {
			panic(err)
		}
	}

	return runtimeCompiler(config)
}

type runtimeCompiler struct {
	converters []ValueConverter
}

func (c runtimeCompiler) Compile(
	expression Expression,
) (BoolExpression, error) {
	tokens, err := scanExpression(string(expression))
	if err != nil {
		return nil, err
	}
	parser := runtimeExpressionParser{
		tokens: tokens,
	}
	node, err := parser.parseExpression(1)
	if err != nil {
		return nil, err
	}
	if parser.peek().typ != runtimeTokenEOF {
		return nil, fmt.Errorf(
			"%w: unexpected token %q",
			ErrInvalid,
			parser.peek().lit,
		)
	}

	return compiledExpression{
		root:       node,
		converters: c.converters,
	}, nil
}

type compiledExpression struct {
	root       runtimeExpressionNode
	converters []ValueConverter
}

func (e compiledExpression) EvaluateBool(request Request) (bool, error) {
	value, err := e.root.evaluate(runtimeExpressionEvalContext{
		request:    request,
		converters: e.converters,
	})
	if err != nil {
		return false, err
	}

	return expressionValueToBool(value)
}

type runtimeExpressionEvalContext struct {
	request    Request
	converters []ValueConverter
}

func (c runtimeExpressionEvalContext) convert(value any) (Value, error) {
	for _, converter := range c.converters {
		converted, handled, err := converter.Convert(ValueConvertRequest{
			Value: value,
		})
		if err != nil {
			return Value{}, err
		}
		if handled {
			return converted, nil
		}
	}

	return Value{Kind: ValueObject, Value: value}, nil
}

func (c runtimeExpressionEvalContext) lookup(path string) (Value, bool, error) {
	if c.request.Variables == nil {
		return Value{}, false, nil
	}

	if typed, ok := c.request.Variables.(runtimevars.TypedVariables); ok {
		value, found, err := typed.LookupValue(path)
		if err != nil || found {
			return expressionValueFromTypedValue(value), found, err
		}
	}

	value, ok := c.request.Variables.Lookup(path)
	if !ok {
		return Value{}, false, nil
	}

	converted, err := c.convert(value)
	return converted, true, err
}
