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
	return reflectionResolver[T]{}
}

type reflectionResolver[T any] struct{}

func (reflectionResolver[T]) Resolve(request ResolveRequest[T]) (any, bool, error) {
	if request.Event == nil {
		return nil, false, nil
	}
	if err := ValidatePath(request.Path); err != nil {
		return nil, false, err
	}

	return resolveReflectPath(reflect.ValueOf(request.Event), strings.Split(request.Path, "."))
}

func resolveReflectPath(value reflect.Value, parts []string) (any, bool, error) {
	value = indirectReflectValue(value)
	if !value.IsValid() {
		return nil, false, nil
	}
	if len(parts) == 0 {
		return value.Interface(), true, nil
	}

	head := parts[0]
	switch value.Kind() {
	case reflect.Map:
		key := reflect.ValueOf(head)
		if key.Type().AssignableTo(value.Type().Key()) {
			item := value.MapIndex(key)
			if !item.IsValid() {
				return nil, false, nil
			}

			return resolveReflectPath(item, parts[1:])
		}
	case reflect.Struct:
		field := findStructField(value, head)
		if field.IsValid() {
			return resolveReflectPath(field, parts[1:])
		}
	}

	return nil, false, nil
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
		if tagName := strings.Split(fieldType.Tag.Get("json"), ",")[0]; tagName == name {
			return value.Field(i)
		}
	}

	return reflect.Value{}
}
