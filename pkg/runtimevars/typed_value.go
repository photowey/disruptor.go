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

package runtimevars

// TypedValueKind identifies a runtime variable value without boxing scalars.
type TypedValueKind uint8

const (
	// TypedValueInvalid is the zero value and is never produced intentionally.
	TypedValueInvalid TypedValueKind = iota
	// TypedValueBool represents a bool value.
	TypedValueBool
	// TypedValueInt represents a signed integer value.
	TypedValueInt
	// TypedValueUint represents an unsigned integer value.
	TypedValueUint
	// TypedValueFloat represents a floating point value.
	TypedValueFloat
	// TypedValueString represents a string value.
	TypedValueString
	// TypedValueObject represents an unsupported object value.
	TypedValueObject
	// TypedValueNil represents nil.
	TypedValueNil
)

// TypedValue stores scalar runtime values without forcing an any allocation.
type TypedValue struct {
	Kind   TypedValueKind
	Bool   bool
	Int    int64
	Uint   uint64
	Float  float64
	String string
	Value  any
}

// TypedVariables exposes runtime variable lookup with scalar-preserving values.
type TypedVariables interface {
	LookupValue(path string) (TypedValue, bool, error)
}

// TypedResolver resolves event values without boxing scalar reflection results.
type TypedResolver[T any] interface {
	ResolveValue(request ResolveRequest[T]) (TypedValue, bool, error)
}

func typedValueFromAny(value any) TypedValue {
	switch typed := value.(type) {
	case nil:
		return TypedValue{Kind: TypedValueNil}
	case bool:
		return TypedValue{Kind: TypedValueBool, Bool: typed}
	case int:
		return TypedValue{Kind: TypedValueInt, Int: int64(typed)}
	case int8:
		return TypedValue{Kind: TypedValueInt, Int: int64(typed)}
	case int16:
		return TypedValue{Kind: TypedValueInt, Int: int64(typed)}
	case int32:
		return TypedValue{Kind: TypedValueInt, Int: int64(typed)}
	case int64:
		return TypedValue{Kind: TypedValueInt, Int: typed}
	case uint:
		return TypedValue{Kind: TypedValueUint, Uint: uint64(typed)}
	case uint8:
		return TypedValue{Kind: TypedValueUint, Uint: uint64(typed)}
	case uint16:
		return TypedValue{Kind: TypedValueUint, Uint: uint64(typed)}
	case uint32:
		return TypedValue{Kind: TypedValueUint, Uint: uint64(typed)}
	case uint64:
		return TypedValue{Kind: TypedValueUint, Uint: typed}
	case uintptr:
		return TypedValue{Kind: TypedValueUint, Uint: uint64(typed)}
	case float32:
		return TypedValue{Kind: TypedValueFloat, Float: float64(typed)}
	case float64:
		return TypedValue{Kind: TypedValueFloat, Float: typed}
	case string:
		return TypedValue{Kind: TypedValueString, String: typed}
	default:
		return TypedValue{Kind: TypedValueObject, Value: value}
	}
}

func (v TypedValue) any() any {
	switch v.Kind {
	case TypedValueBool:
		return v.Bool
	case TypedValueInt:
		return v.Int
	case TypedValueUint:
		return v.Uint
	case TypedValueFloat:
		return v.Float
	case TypedValueString:
		return v.String
	case TypedValueNil:
		return nil
	default:
		return v.Value
	}
}
