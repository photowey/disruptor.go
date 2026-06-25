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
	"math"
)

type runtimeExpressionNode interface {
	evaluate(context runtimeExpressionEvalContext) (Value, error)
}

type runtimeLiteralNode struct {
	value Value
}

func (n runtimeLiteralNode) evaluate(
	context runtimeExpressionEvalContext,
) (Value, error) {
	return n.value, nil
}

type runtimePathNode struct {
	path string
}

func (n runtimePathNode) evaluate(
	context runtimeExpressionEvalContext,
) (Value, error) {
	value, ok, err := context.lookup(n.path)
	if err != nil {
		return Value{}, err
	}
	if !ok {
		return Value{}, fmt.Errorf(
			"%w: runtime path %q not found",
			ErrInvalid,
			n.path,
		)
	}

	return value, nil
}

type runtimeUnaryNode struct {
	op   string
	node runtimeExpressionNode
}

func (n runtimeUnaryNode) evaluate(
	context runtimeExpressionEvalContext,
) (Value, error) {
	value, err := n.node.evaluate(context)
	if err != nil {
		return Value{}, err
	}

	switch n.op {
	case "!":
		boolean, err := requireExpressionBool(value)
		if err != nil {
			return Value{}, err
		}

		return Value{Kind: ValueBool, Bool: !boolean}, nil
	default:
		return Value{}, fmt.Errorf(
			"%w: unsupported unary operator %q",
			ErrInvalid,
			n.op,
		)
	}
}

type runtimeBinaryNode struct {
	op    string
	left  runtimeExpressionNode
	right runtimeExpressionNode
}

func (n runtimeBinaryNode) evaluate(
	context runtimeExpressionEvalContext,
) (Value, error) {
	left, err := n.left.evaluate(context)
	if err != nil {
		return Value{}, err
	}

	switch n.op {
	case "&&":
		leftBool, err := requireExpressionBool(left)
		if err != nil {
			return Value{}, err
		}
		if !leftBool {
			return Value{Kind: ValueBool, Bool: false}, nil
		}
		right, err := n.right.evaluate(context)
		if err != nil {
			return Value{}, err
		}
		rightBool, err := requireExpressionBool(right)
		if err != nil {
			return Value{}, err
		}

		return Value{Kind: ValueBool, Bool: rightBool}, nil
	case "||":
		leftBool, err := requireExpressionBool(left)
		if err != nil {
			return Value{}, err
		}
		if leftBool {
			return Value{Kind: ValueBool, Bool: true}, nil
		}
		right, err := n.right.evaluate(context)
		if err != nil {
			return Value{}, err
		}
		rightBool, err := requireExpressionBool(right)
		if err != nil {
			return Value{}, err
		}

		return Value{Kind: ValueBool, Bool: rightBool}, nil
	default:
		right, err := n.right.evaluate(context)
		if err != nil {
			return Value{}, err
		}

		return evaluateRuntimeBinaryOperator(n.op, left, right)
	}
}

func evaluateRuntimeBinaryOperator(
	op string,
	left Value,
	right Value,
) (Value, error) {
	switch op {
	case "==", "!=", ">", ">=", "<", "<=":
		return evaluateRuntimeComparison(op, left, right)
	case "&", "|", "^", "&^", "<<", ">>":
		return evaluateRuntimeBitwise(op, left, right)
	default:
		return Value{}, fmt.Errorf(
			"%w: unsupported binary operator %q",
			ErrInvalid,
			op,
		)
	}
}

func evaluateRuntimeComparison(
	op string,
	left Value,
	right Value,
) (Value, error) {
	if left.Kind == ValueNil || right.Kind == ValueNil {
		result := left.Kind == right.Kind
		if op == "!=" {
			result = !result
		} else if op != "==" {
			return Value{}, fmt.Errorf(
				"%w: nil only supports == and !=",
				ErrInvalid,
			)
		}

		return Value{Kind: ValueBool, Bool: result}, nil
	}
	if isExpressionNumeric(left) && isExpressionNumeric(right) {
		result := compareRuntimeNumeric(op, left, right)
		return Value{Kind: ValueBool, Bool: result}, nil
	}
	if left.Kind == ValueString && right.Kind == ValueString {
		result := compareRuntimeString(op, expressionString(left), expressionString(right))
		return Value{Kind: ValueBool, Bool: result}, nil
	}
	if left.Kind == ValueBool && right.Kind == ValueBool {
		result, err := compareRuntimeBool(op, expressionBool(left), expressionBool(right))
		if err != nil {
			return Value{}, err
		}

		return Value{Kind: ValueBool, Bool: result}, nil
	}

	return Value{}, fmt.Errorf(
		"%w: cannot compare %v and %v",
		ErrInvalid,
		left.Kind,
		right.Kind,
	)
}

func evaluateRuntimeBitwise(
	op string,
	left Value,
	right Value,
) (Value, error) {
	leftUint, leftSigned, ok := expressionIntegerUint(left)
	if !ok {
		return Value{}, fmt.Errorf(
			"%w: bitwise left operand is not an integer",
			ErrInvalid,
		)
	}
	rightUint, rightSigned, ok := expressionIntegerUint(right)
	if !ok {
		return Value{}, fmt.Errorf(
			"%w: bitwise right operand is not an integer",
			ErrInvalid,
		)
	}

	var result uint64
	switch op {
	case "&":
		result = leftUint & rightUint
	case "|":
		result = leftUint | rightUint
	case "^":
		result = leftUint ^ rightUint
	case "&^":
		result = leftUint &^ rightUint
	case "<<":
		result = leftUint << rightUint
	case ">>":
		result = leftUint >> rightUint
	}
	if leftSigned && rightSigned && result <= math.MaxInt64 {
		return Value{Kind: ValueInt, Int: int64(result)}, nil
	}

	return Value{Kind: ValueUint, Uint: result}, nil
}
