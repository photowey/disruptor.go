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
	"strconv"

	"github.com/photowey/disruptor.go/pkg/runtimevars"
)

func expressionValueToBool(value Value) (bool, error) {
	switch value.Kind {
	case ValueBool:
		return expressionBool(value), nil
	case ValueInt:
		return expressionInt(value) != 0, nil
	case ValueUint:
		return expressionUint(value) != 0, nil
	case ValueString:
		raw := expressionString(value)
		boolean, err := strconv.ParseBool(raw)
		if err != nil {
			return false, fmt.Errorf(
				"%w: cannot convert string %q to bool",
				ErrInvalid,
				raw,
			)
		}

		return boolean, nil
	default:
		return false, fmt.Errorf(
			"%w: cannot convert %v to bool",
			ErrInvalid,
			value.Kind,
		)
	}
}

func requireExpressionBool(value Value) (bool, error) {
	if value.Kind != ValueBool {
		return false, fmt.Errorf(
			"%w: logical operand must be bool, got %v",
			ErrInvalid,
			value.Kind,
		)
	}

	return expressionBool(value), nil
}

func isExpressionNumeric(value Value) bool {
	return value.Kind == ValueInt ||
		value.Kind == ValueUint ||
		value.Kind == ValueFloat
}

func compareRuntimeNumeric(op string, left Value, right Value) bool {
	if left.Kind == ValueFloat || right.Kind == ValueFloat {
		return compareRuntimeFloat(
			op,
			expressionNumericFloat(left),
			expressionNumericFloat(right),
		)
	}
	if left.Kind == ValueInt && right.Kind == ValueInt {
		return compareRuntimeInt(op, expressionInt(left), expressionInt(right))
	}
	if left.Kind == ValueUint && right.Kind == ValueUint {
		return compareRuntimeUint(op, expressionUint(left), expressionUint(right))
	}
	if left.Kind == ValueInt {
		return compareRuntimeSignedUnsigned(op, expressionInt(left), expressionUint(right))
	}

	return compareRuntimeUnsignedSigned(op, expressionUint(left), expressionInt(right))
}

func expressionNumericFloat(value Value) float64 {
	switch value.Kind {
	case ValueInt:
		return float64(expressionInt(value))
	case ValueUint:
		return float64(expressionUint(value))
	case ValueFloat:
		return expressionFloat(value)
	default:
		return 0
	}
}

func expressionIntegerUint(value Value) (uint64, bool, bool) {
	switch value.Kind {
	case ValueInt:
		integer := expressionInt(value)
		if integer < 0 {
			return 0, true, false
		}

		return uint64(integer), true, true
	case ValueUint:
		return expressionUint(value), false, true
	default:
		return 0, false, false
	}
}

func compareRuntimeInt(op string, left int64, right int64) bool {
	switch op {
	case "==":
		return left == right
	case "!=":
		return left != right
	case ">":
		return left > right
	case ">=":
		return left >= right
	case "<":
		return left < right
	case "<=":
		return left <= right
	default:
		return false
	}
}

func compareRuntimeUint(op string, left uint64, right uint64) bool {
	switch op {
	case "==":
		return left == right
	case "!=":
		return left != right
	case ">":
		return left > right
	case ">=":
		return left >= right
	case "<":
		return left < right
	case "<=":
		return left <= right
	default:
		return false
	}
}

func compareRuntimeSignedUnsigned(op string, left int64, right uint64) bool {
	if left < 0 {
		switch op {
		case "==":
			return false
		case "!=":
			return true
		case ">":
			return false
		case ">=":
			return false
		case "<":
			return true
		case "<=":
			return true
		default:
			return false
		}
	}

	return compareRuntimeUint(op, uint64(left), right)
}

func compareRuntimeUnsignedSigned(op string, left uint64, right int64) bool {
	if right < 0 {
		switch op {
		case "==":
			return false
		case "!=":
			return true
		case ">":
			return true
		case ">=":
			return true
		case "<":
			return false
		case "<=":
			return false
		default:
			return false
		}
	}

	return compareRuntimeUint(op, left, uint64(right))
}

func expressionBool(value Value) bool {
	if value.Value != nil {
		return value.Value.(bool)
	}

	return value.Bool
}

func expressionInt(value Value) int64 {
	if value.Value != nil {
		return value.Value.(int64)
	}

	return value.Int
}

func expressionUint(value Value) uint64 {
	if value.Value != nil {
		return value.Value.(uint64)
	}

	return value.Uint
}

func expressionFloat(value Value) float64 {
	if value.Value != nil {
		return value.Value.(float64)
	}

	return value.Float
}

func expressionString(value Value) string {
	if value.Value != nil {
		return value.Value.(string)
	}

	return value.String
}

func compareRuntimeFloat(op string, left float64, right float64) bool {
	switch op {
	case "==":
		return left == right
	case "!=":
		return left != right
	case ">":
		return left > right
	case ">=":
		return left >= right
	case "<":
		return left < right
	case "<=":
		return left <= right
	default:
		return false
	}
}

func compareRuntimeString(op string, left string, right string) bool {
	switch op {
	case "==":
		return left == right
	case "!=":
		return left != right
	case ">":
		return left > right
	case ">=":
		return left >= right
	case "<":
		return left < right
	case "<=":
		return left <= right
	default:
		return false
	}
}

func compareRuntimeBool(op string, left bool, right bool) (bool, error) {
	switch op {
	case "==":
		return left == right, nil
	case "!=":
		return left != right, nil
	default:
		return false, fmt.Errorf(
			"%w: bool only supports == and !=",
			ErrInvalid,
		)
	}
}

func defaultValueConverters() []ValueConverter {
	return []ValueConverter{
		ValueConverterFunc(convertRuntimeNilValue),
		ValueConverterFunc(convertRuntimeBoolValue),
		ValueConverterFunc(convertRuntimeSignedIntValue),
		ValueConverterFunc(convertRuntimeUnsignedIntValue),
		ValueConverterFunc(convertRuntimeFloatValue),
		ValueConverterFunc(convertRuntimeStringValue),
	}
}

func convertRuntimeNilValue(request ValueConvertRequest) (Value, bool, error) {
	if request.Value == nil {
		return Value{Kind: ValueNil}, true, nil
	}

	return Value{}, false, nil
}

func convertRuntimeBoolValue(request ValueConvertRequest) (Value, bool, error) {
	value, ok := request.Value.(bool)
	if !ok {
		return Value{}, false, nil
	}

	return Value{Kind: ValueBool, Bool: value}, true, nil
}

func convertRuntimeSignedIntValue(
	request ValueConvertRequest,
) (Value, bool, error) {
	switch value := request.Value.(type) {
	case int:
		return Value{Kind: ValueInt, Int: int64(value)}, true, nil
	case int8:
		return Value{Kind: ValueInt, Int: int64(value)}, true, nil
	case int16:
		return Value{Kind: ValueInt, Int: int64(value)}, true, nil
	case int32:
		return Value{Kind: ValueInt, Int: int64(value)}, true, nil
	case int64:
		return Value{Kind: ValueInt, Int: value}, true, nil
	default:
		return Value{}, false, nil
	}
}

func convertRuntimeUnsignedIntValue(
	request ValueConvertRequest,
) (Value, bool, error) {
	switch value := request.Value.(type) {
	case uint:
		return Value{Kind: ValueUint, Uint: uint64(value)}, true, nil
	case uint8:
		return Value{Kind: ValueUint, Uint: uint64(value)}, true, nil
	case uint16:
		return Value{Kind: ValueUint, Uint: uint64(value)}, true, nil
	case uint32:
		return Value{Kind: ValueUint, Uint: uint64(value)}, true, nil
	case uint64:
		return Value{Kind: ValueUint, Uint: value}, true, nil
	case uintptr:
		return Value{Kind: ValueUint, Uint: uint64(value)}, true, nil
	default:
		return Value{}, false, nil
	}
}

func convertRuntimeFloatValue(
	request ValueConvertRequest,
) (Value, bool, error) {
	switch value := request.Value.(type) {
	case float32:
		return Value{Kind: ValueFloat, Float: float64(value)}, true, nil
	case float64:
		return Value{Kind: ValueFloat, Float: value}, true, nil
	default:
		return Value{}, false, nil
	}
}

func convertRuntimeStringValue(
	request ValueConvertRequest,
) (Value, bool, error) {
	value, ok := request.Value.(string)
	if !ok {
		return Value{}, false, nil
	}

	return Value{Kind: ValueString, String: value}, true, nil
}

func expressionValueFromTypedValue(value runtimevars.TypedValue) Value {
	switch value.Kind {
	case runtimevars.TypedValueBool:
		return Value{Kind: ValueBool, Bool: value.Bool}
	case runtimevars.TypedValueInt:
		return Value{Kind: ValueInt, Int: value.Int}
	case runtimevars.TypedValueUint:
		return Value{Kind: ValueUint, Uint: value.Uint}
	case runtimevars.TypedValueFloat:
		return Value{Kind: ValueFloat, Float: value.Float}
	case runtimevars.TypedValueString:
		return Value{Kind: ValueString, String: value.String}
	case runtimevars.TypedValueNil:
		return Value{Kind: ValueNil}
	default:
		return Value{Kind: ValueObject, Value: value.Value}
	}
}
