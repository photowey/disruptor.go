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

import (
	"context"
	"reflect"
	"strings"
	"sync"
)

// Resolver resolves expression paths from an event value.
type Resolver[T any] interface {
	Resolve(request ResolveRequest[T]) (any, bool, error)
}

// ResolverFunc adapts a function to Resolver.
type ResolverFunc[T any] func(request ResolveRequest[T]) (any, bool, error)

// Resolve calls the wrapped resolver function.
func (fn ResolverFunc[T]) Resolve(request ResolveRequest[T]) (any, bool, error) {
	if fn == nil {
		return nil, false, nil
	}

	return fn(request)
}

// ResolveRequest describes an event field lookup.
type ResolveRequest[T any] struct {
	Context   context.Context
	Event     *T
	Sequence  int64
	GraphName string
	Path      string
}

// NewReflectionResolver creates a resolver that reads fields, JSON tags, and string maps.
func NewReflectionResolver[T any]() Resolver[T] {
	return &reflectionResolver[T]{}
}

type reflectionResolver[T any] struct {
	paths sync.Map
}

func (r *reflectionResolver[T]) Resolve(request ResolveRequest[T]) (any, bool, error) {
	value, ok, err := r.ResolveValue(request)
	if err != nil || !ok {
		return nil, ok, err
	}

	return value.any(), true, nil
}

func (r *reflectionResolver[T]) ResolveValue(
	request ResolveRequest[T],
) (TypedValue, bool, error) {
	if request.Event == nil {
		return TypedValue{}, false, nil
	}

	path, err := r.path(request.Path)
	if err != nil {
		return TypedValue{}, false, err
	}

	return resolveReflectPath(reflect.ValueOf(request.Event), path, 0)
}

func (r *reflectionResolver[T]) path(path string) (compiledPath, error) {
	if r == nil {
		return compilePath(path)
	}
	if cached, ok := r.paths.Load(path); ok {
		return cached.(compiledPath), nil
	}

	compiled, err := compilePath(path)
	if err != nil {
		return compiledPath{}, err
	}
	cached, _ := r.paths.LoadOrStore(path, compiled)
	return cached.(compiledPath), nil
}

func resolveReflectPath(
	value reflect.Value,
	path compiledPath,
	index int,
) (TypedValue, bool, error) {
	value = indirectReflectValue(value)
	if !value.IsValid() {
		return TypedValue{}, false, nil
	}
	if index >= path.Len() {
		return reflectTypedValue(value)
	}

	head := path.At(index)
	switch value.Kind() {
	case reflect.Map:
		key := reflect.ValueOf(head)
		if key.Type().AssignableTo(value.Type().Key()) {
			item := value.MapIndex(key)
			if !item.IsValid() {
				return TypedValue{}, false, nil
			}

			return resolveReflectPath(item, path, index+1)
		}
	case reflect.Struct:
		field := findStructField(value, head)
		if field.IsValid() {
			return resolveReflectPath(field, path, index+1)
		}
	}

	return TypedValue{}, false, nil
}

func reflectTypedValue(value reflect.Value) (TypedValue, bool, error) {
	switch value.Kind() {
	case reflect.Bool:
		return TypedValue{Kind: TypedValueBool, Bool: value.Bool()}, true, nil
	case reflect.Int:
		return TypedValue{Kind: TypedValueInt, Int: value.Int()}, true, nil
	case reflect.Int8:
		return TypedValue{Kind: TypedValueInt, Int: value.Int()}, true, nil
	case reflect.Int16:
		return TypedValue{Kind: TypedValueInt, Int: value.Int()}, true, nil
	case reflect.Int32:
		return TypedValue{Kind: TypedValueInt, Int: value.Int()}, true, nil
	case reflect.Int64:
		return TypedValue{Kind: TypedValueInt, Int: value.Int()}, true, nil
	case reflect.Uint:
		return TypedValue{Kind: TypedValueUint, Uint: value.Uint()}, true, nil
	case reflect.Uint8:
		return TypedValue{Kind: TypedValueUint, Uint: value.Uint()}, true, nil
	case reflect.Uint16:
		return TypedValue{Kind: TypedValueUint, Uint: value.Uint()}, true, nil
	case reflect.Uint32:
		return TypedValue{Kind: TypedValueUint, Uint: value.Uint()}, true, nil
	case reflect.Uint64:
		return TypedValue{Kind: TypedValueUint, Uint: value.Uint()}, true, nil
	case reflect.Uintptr:
		return TypedValue{Kind: TypedValueUint, Uint: value.Uint()}, true, nil
	case reflect.Float32:
		return TypedValue{Kind: TypedValueFloat, Float: value.Float()}, true, nil
	case reflect.Float64:
		return TypedValue{Kind: TypedValueFloat, Float: value.Float()}, true, nil
	case reflect.String:
		return TypedValue{Kind: TypedValueString, String: value.String()}, true, nil
	default:
		if value.CanInterface() {
			return TypedValue{
				Kind:  TypedValueObject,
				Value: value.Interface(),
			}, true, nil
		}

		return TypedValue{}, false, nil
	}
}

func indirectReflectValue(value reflect.Value) reflect.Value {
	for value.IsValid() && (value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface) {
		if value.IsNil() {
			return reflect.Value{}
		}
		value = value.Elem()
	}

	return value
}

func findStructField(value reflect.Value, name string) reflect.Value {
	valueType := value.Type()
	for i := 0; i < value.NumField(); i++ {
		fieldType := valueType.Field(i)
		if fieldType.PkgPath != "" {
			continue
		}
		if fieldType.Name == name || strings.EqualFold(fieldType.Name, name) {
			return value.Field(i)
		}
		if tagName, _, _ := strings.Cut(fieldType.Tag.Get("json"), ","); tagName == name {
			return value.Field(i)
		}
	}

	return reflect.Value{}
}
