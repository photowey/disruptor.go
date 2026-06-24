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
)

func expressionValueToBool(value Value) (bool, error) {
	switch value.Kind {
	case ValueBool:
		return value.Value.(bool), nil
	case ValueInt:
		return value.Value.(int64) != 0, nil
	case ValueUint:
		return value.Value.(uint64) != 0, nil
	case ValueString:
		boolean, err := strconv.ParseBool(value.Value.(string))
		if err != nil {
			return false, fmt.Errorf(
				"%w: cannot convert string %q to bool",
				ErrInvalid,
				value.Value.(string),
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

	return value.Value.(bool), nil
}

func isExpressionNumeric(value Value) bool {
	return value.Kind == ValueInt ||
		value.Kind == ValueUint ||
		value.Kind == ValueFloat
}

func expressionNumericFloat(value Value) float64 {
	switch value.Kind {
	case ValueInt:
		return float64(value.Value.(int64))
	case ValueUint:
		return float64(value.Value.(uint64))
	case ValueFloat:
		return value.Value.(float64)
	default:
		return 0
	}
}

func expressionIntegerUint(value Value) (uint64, bool, bool) {
	switch value.Kind {
	case ValueInt:
		integer := value.Value.(int64)
		if integer < 0 {
			return 0, true, false
		}

		return uint64(integer), true, true
	case ValueUint:
		return value.Value.(uint64), false, true
	default:
		return 0, false, false
	}
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
		return Value{Kind: ValueNil, Value: nil}, true, nil
	}

	return Value{}, false, nil
}

func convertRuntimeBoolValue(request ValueConvertRequest) (Value, bool, error) {
	value, ok := request.Value.(bool)
	if !ok {
		return Value{}, false, nil
	}

	return Value{Kind: ValueBool, Value: value}, true, nil
}

func convertRuntimeSignedIntValue(
	request ValueConvertRequest,
) (Value, bool, error) {
	switch value := request.Value.(type) {
	case int:
		return Value{Kind: ValueInt, Value: int64(value)}, true, nil
	case int8:
		return Value{Kind: ValueInt, Value: int64(value)}, true, nil
	case int16:
		return Value{Kind: ValueInt, Value: int64(value)}, true, nil
	case int32:
		return Value{Kind: ValueInt, Value: int64(value)}, true, nil
	case int64:
		return Value{Kind: ValueInt, Value: value}, true, nil
	default:
		return Value{}, false, nil
	}
}

func convertRuntimeUnsignedIntValue(
	request ValueConvertRequest,
) (Value, bool, error) {
	switch value := request.Value.(type) {
	case uint:
		return Value{Kind: ValueUint, Value: uint64(value)}, true, nil
	case uint8:
		return Value{Kind: ValueUint, Value: uint64(value)}, true, nil
	case uint16:
		return Value{Kind: ValueUint, Value: uint64(value)}, true, nil
	case uint32:
		return Value{Kind: ValueUint, Value: uint64(value)}, true, nil
	case uint64:
		return Value{Kind: ValueUint, Value: value}, true, nil
	case uintptr:
		return Value{Kind: ValueUint, Value: uint64(value)}, true, nil
	default:
		return Value{}, false, nil
	}
}

func convertRuntimeFloatValue(
	request ValueConvertRequest,
) (Value, bool, error) {
	switch value := request.Value.(type) {
	case float32:
		return Value{Kind: ValueFloat, Value: float64(value)}, true, nil
	case float64:
		return Value{Kind: ValueFloat, Value: value}, true, nil
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

	return Value{Kind: ValueString, Value: value}, true, nil
}
